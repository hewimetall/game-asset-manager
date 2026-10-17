#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

use eframe::egui;
use stand_sync::config::{load_config, Config};
use stand_sync::index::Index;
use stand_sync::lock::{try_lock, InstanceLock, LockError};
use stand_sync::logsetup::{init_logging, LogHandles, LogState};
use stand_sync::pass::{run_pass, PassOptions};
use stand_sync::paths::{exe_directory, exe_file_name, fmt_mib, probe_workdir};
use stand_sync::retry::backoff_delay;
use stand_sync::s3::S3Store;
use stand_sync::store::{access_failure_message, check_listing, ObjectStore};
use stand_sync::ui_model::{fraction, Phase, UiModel};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    let workdir = match exe_directory() {
        Ok(dir) => dir,
        Err(err) => {
            tell(&err);
            return;
        }
    };
    let instance = match try_lock(&workdir) {
        Ok(lock) => lock,
        Err(LockError::AlreadyRunning) => {
            tell("Уже запущена другая копия синхронизации.");
            return;
        }
        Err(LockError::Failed(message)) => {
            tell(&message);
            return;
        }
    };
    let log = init_logging(&workdir).ok();
    if log.is_some() {
        tracing::info!(dir = %workdir.display(), "запуск");
    }
    let startup = prepare(&workdir);
    if let (Some(handles), Some(config)) = (&log, &startup.config) {
        handles.state.remember_secret(&config.access_key);
        handles.state.remember_secret(&config.secret_key);
    }
    if let Some(message) = &startup.fatal {
        tracing::error!(error = %message, "запуск остановлен");
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            tell(&format!("не удалось запустить среду выполнения: {err}"));
            return;
        }
    };
    let app = SyncApp {
        rt,
        workdir: workdir.clone(),
        exe_name: exe_file_name(&workdir),
        log_path: workdir.join("logs.txt"),
        _instance: instance,
        log,
        model: Arc::new(Mutex::new(UiModel::new(startup_stand(&startup)))),
        startup,
        speed_at: None,
        speed_bytes: 0,
        speed_ema: 0.0,
        cancel: Arc::new(AtomicBool::new(false)),
    };
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([520.0, 700.0])
            .with_min_inner_size([420.0, 520.0]),
        ..Default::default()
    };
    if let Err(err) = eframe::run_native(
        "Синхронизация стенда",
        options,
        Box::new(|cc| {
            install_fonts(&cc.egui_ctx);
            cc.egui_ctx.set_visuals(egui::Visuals::light());
            Ok(Box::new(app))
        }),
    ) {
        tell(&format!("окно не открылось: {err}"));
    }
}

struct Startup {
    fatal: Option<String>,
    config: Option<Config>,
}

fn startup_stand(startup: &Startup) -> String {
    startup
        .config
        .as_ref()
        .map(|config| config.stand.clone())
        .unwrap_or_default()
}

fn prepare(workdir: &PathBuf) -> Startup {
    let config = match load_config(workdir) {
        Ok(config) => config,
        Err(err) => {
            return Startup {
                fatal: Some(err.to_string()),
                config: None,
            };
        }
    };
    if let Err(err) = probe_workdir(workdir) {
        return Startup {
            fatal: Some(err),
            config: Some(config),
        };
    }
    if let Err(err) = Index::open(&Index::path_in(workdir), &config.profile()) {
        return Startup {
            fatal: Some(err.to_string()),
            config: Some(config),
        };
    }
    Startup {
        fatal: None,
        config: Some(config),
    }
}

struct SyncApp {
    rt: tokio::runtime::Runtime,
    workdir: PathBuf,
    exe_name: String,
    log_path: PathBuf,
    _instance: InstanceLock,
    log: Option<LogHandles>,
    startup: Startup,
    model: Arc<Mutex<UiModel>>,
    speed_at: Option<Instant>,
    speed_bytes: u64,
    speed_ema: f64,
    cancel: Arc<AtomicBool>,
}

impl eframe::App for SyncApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let model = self
            .model
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        if model.syncing || model.waiting_until.is_some() {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
        self.sample_speed(
            model.bytes_done,
            model.syncing && model.phase == Phase::Transferring,
        );
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.spacing_mut().item_spacing.y = 10.0;
            let stand = self
                .startup
                .config
                .as_ref()
                .map(|config| config.stand.as_str())
                .unwrap_or("—");
            ui.label(egui::RichText::new(format!("Stand {stand}")).size(22.0));
            if let Some(message) = &self.startup.fatal {
                ui.colored_label(egui::Color32::from_rgb(170, 40, 40), message);
                self.log_link(ui);
                return;
            }
            ui.label("Ключи читаются из config.toml.");
            let syncing = model.syncing;
            if ui
                .add_enabled(
                    !syncing,
                    egui::Button::new("Проверить доступ").min_size(egui::vec2(220.0, 32.0)),
                )
                .clicked()
            {
                self.start_check();
                ctx.request_repaint();
            }
            if ui
                .add_enabled(
                    !syncing,
                    egui::Button::new("Синхронизировать").min_size(egui::vec2(220.0, 32.0)),
                )
                .clicked()
            {
                self.start_sync();
                ctx.request_repaint();
            }
            self.progress_view(ui, &model);
            if !model.status.is_empty() && model.phase != Phase::Scanning {
                ui.label(&model.status);
            }
            if !model.conflicts.is_empty() {
                ui.separator();
                ui.label(format!(
                    "Конфликты: {}. Ничего не перезаписано.",
                    model.conflicts.len()
                ));
                for note in model.conflicts.iter().take(30) {
                    ui.label(format!("• {} — {}", note.rel, note.reason));
                }
            }
            if model.degraded && syncing {
                ui.label("Нестабильная сеть: одна передача.");
            }
            self.log_link(ui);
        });
    }
}

impl SyncApp {
    fn progress_view(&mut self, ui: &mut egui::Ui, model: &UiModel) {
        if model.phase == Phase::Scanning {
            ui.label("Проверяем изменения…");
            return;
        }
        if let Some(value) = fraction(model) {
            let percent = (value * 100.0).round() as u32;
            ui.add(egui::ProgressBar::new(value).show_percentage());
            ui.label(format!("{percent}%"));
        }
        if model.phase == Phase::Transferring
            || model.phase == Phase::Done
            || model.phase == Phase::Failed
        {
            if model.files_total > 0 || model.phase != Phase::Failed {
                ui.label(format!(
                    "готово {} из {} файлов",
                    model.files_done, model.files_total
                ));
                ui.label(format!(
                    "передано {} из {} МиБ",
                    fmt_mib(model.bytes_done),
                    fmt_mib(model.bytes_total)
                ));
            }
        }
        if let Some(until) = model.waiting_until {
            let left = until.saturating_duration_since(Instant::now());
            ui.label(format!("Ожидаем сеть. Повтор через {}", fmt_wait(left)));
        } else if model.phase == Phase::Transferring
            && model.bytes_total > model.bytes_done
            && self.speed_ema > 1.0
        {
            let remain = (model.bytes_total - model.bytes_done) as f64 / self.speed_ema;
            ui.label(format!(
                "{} МиБ/с · осталось {}",
                fmt_mib(self.speed_ema.round() as u64),
                fmt_wait(Duration::from_secs_f64(remain))
            ));
        }
        for file in model.current.iter().take(2) {
            ui.label(format!(
                "{}  {}  {} / {} МиБ",
                file.direction.label(),
                file.rel,
                fmt_mib(file.done),
                fmt_mib(file.total)
            ));
        }
    }

    fn log_link(&self, ui: &mut egui::Ui) {
        if ui.link("Открыть лог").clicked() {
            open_path(&self.log_path);
        }
    }

    fn sample_speed(&mut self, bytes: u64, active: bool) {
        let now = Instant::now();
        if !active {
            if bytes == 0 {
                self.speed_at = None;
                self.speed_bytes = 0;
                self.speed_ema = 0.0;
            }
            return;
        }
        if let Some(previous) = self.speed_at {
            let dt = now.duration_since(previous).as_secs_f64();
            if dt >= 0.25 {
                let delta = bytes.saturating_sub(self.speed_bytes) as f64;
                let instant = delta / dt;
                self.speed_ema = if self.speed_ema == 0.0 {
                    instant
                } else {
                    instant * 0.25 + self.speed_ema * 0.75
                };
                self.speed_bytes = bytes;
                self.speed_at = Some(now);
            }
        } else {
            self.speed_at = Some(now);
            self.speed_bytes = bytes;
        }
    }

    fn start_check(&mut self) {
        if self
            .model
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .syncing
        {
            return;
        }
        let Some(config) = self.startup.config.clone() else {
            return;
        };
        self.remember_config_secrets(&config);
        self.set_busy(true, "Проверяем доступ…");
        let model = Arc::clone(&self.model);
        let prefix = config.prefix();
        let keys = config.keys();
        let state = self.log.as_ref().map(|handles| Arc::clone(&handles.state));
        self.rt.spawn(async move {
            let store = match S3Store::new(&config.profile(), &config.region, &keys) {
                Ok(store) => store,
                Err(err) => {
                    finish_check(&model, err);
                    return;
                }
            };
            match check_listing(&store, &prefix).await {
                Ok(()) => finish_check(&model, "Доступ к префиксу есть.".into()),
                Err(err) => {
                    if let Some(state) = &state {
                        state.push_error(&access_failure_message(&err));
                    }
                    tracing::warn!(
                        status = err.http_status().unwrap_or(0),
                        code = err.s3_code().unwrap_or(""),
                        request_id = err.request_id().unwrap_or(""),
                        "проверка доступа не прошла"
                    );
                    finish_check(&model, access_failure_message(&err));
                }
            }
        });
    }

    fn start_sync(&mut self) {
        if self
            .model
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .syncing
        {
            return;
        }
        tracing::info!("нажата кнопка синхронизации");
        let Some(config) = self.startup.config.clone() else {
            self.set_status("Нет config.toml, синхронизация не запущена.");
            return;
        };
        let keys = config.keys();
        self.remember_config_secrets(&config);
        self.speed_at = None;
        self.speed_bytes = 0;
        self.speed_ema = 0.0;
        {
            let mut model = self.model.lock().unwrap_or_else(|err| err.into_inner());
            model.syncing = true;
            model.phase = Phase::Scanning;
            model.finalized = false;
            model.status.clear();
            model.conflicts.clear();
            model.current.clear();
        }
        let model = Arc::clone(&self.model);
        let workdir = self.workdir.clone();
        let exe_name = self.exe_name.clone();
        let log = self
            .log
            .as_ref()
            .map(|handles| Arc::clone(&handles.state))
            .unwrap_or_else(|| Arc::new(LogState::new()));
        let cancel = Arc::clone(&self.cancel);
        self.rt.spawn(async move {
            let store: Arc<dyn ObjectStore> =
                match S3Store::new(&config.profile(), &config.region, &keys) {
                    Ok(store) => Arc::new(store),
                    Err(err) => {
                        let mut ui = model.lock().unwrap_or_else(|poison| poison.into_inner());
                        ui.syncing = false;
                        ui.phase = Phase::Failed;
                        ui.status = err;
                        return;
                    }
                };
            run_pass(PassOptions {
                workdir,
                exe_name,
                profile: config.profile(),
                prefix: config.prefix(),
                store,
                ui: model,
                log,
                cancel,
                backoff: backoff_delay,
                initial_slots: 2,
            })
            .await;
        });
    }

    fn set_busy(&self, busy: bool, status: &str) {
        let mut model = self.model.lock().unwrap_or_else(|err| err.into_inner());
        model.syncing = busy;
        model.status = status.to_string();
    }

    fn set_status(&self, status: impl Into<String>) {
        self.model
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .status = status.into();
    }

    fn remember_config_secrets(&self, config: &Config) {
        if let Some(handles) = &self.log {
            handles.state.remember_secret(&config.access_key);
            handles.state.remember_secret(&config.secret_key);
        }
    }
}

fn finish_check(model: &Mutex<UiModel>, status: String) {
    let mut ui = model.lock().unwrap_or_else(|err| err.into_inner());
    ui.syncing = false;
    ui.status = status;
    ui.phase = Phase::Idle;
}

fn fmt_wait(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{secs} с")
    } else if secs < 3600 {
        format!("{} мин", (secs + 59) / 60)
    } else {
        format!("{} ч", (secs + 3599) / 3600)
    }
}

fn install_fonts(ctx: &egui::Context) {
    for path in [
        r"C:\Windows\Fonts\segoeui.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ] {
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let mut fonts = egui::FontDefinitions::default();
        fonts
            .font_data
            .insert("ui".into(), egui::FontData::from_owned(bytes));
        fonts
            .families
            .entry(egui::FontFamily::Proportional)
            .or_default()
            .insert(0, "ui".into());
        fonts
            .families
            .entry(egui::FontFamily::Monospace)
            .or_default()
            .insert(0, "ui".into());
        ctx.set_fonts(fonts);
        break;
    }
}

fn open_path(path: &std::path::Path) {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::UI::Shell::ShellExecuteW;
        use windows_sys::Win32::UI::WindowsAndMessaging::SW_SHOWNORMAL;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let op: Vec<u16> =
            std::os::windows::ffi::OsStrExt::encode_wide(std::ffi::OsStr::new("open"))
                .chain(Some(0))
                .collect();
        unsafe {
            ShellExecuteW(
                std::ptr::null_mut(),
                op.as_ptr(),
                wide.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                SW_SHOWNORMAL,
            );
        }
    }
    #[cfg(not(windows))]
    {
        let _ = std::process::Command::new("xdg-open").arg(path).spawn();
    }
}

fn tell(text: &str) {
    tracing::error!("{text}");
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::UI::WindowsAndMessaging::{MessageBoxW, MB_ICONINFORMATION, MB_OK};
        let message: Vec<u16> = std::ffi::OsStr::new(text)
            .encode_wide()
            .chain(Some(0))
            .collect();
        let title: Vec<u16> = std::ffi::OsStr::new("Синхронизация стенда")
            .encode_wide()
            .chain(Some(0))
            .collect();
        unsafe {
            MessageBoxW(
                std::ptr::null_mut(),
                message.as_ptr(),
                title.as_ptr(),
                MB_OK | MB_ICONINFORMATION,
            );
        }
    }
    #[cfg(not(windows))]
    {
        eprintln!("{text}");
    }
}
