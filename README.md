# Синхронизация стенда

Портативная программа для Windows: локальная папка ↔ префикс `stand-<stand>/` в бакете Timeweb S3.

Дизайнеру не нужен установщик. Рядом с `sync.exe` кладётся `config.toml` из `config.example.toml`. Access key и Secret key пишутся в этот файл, в секции `[s3]`. В `logs.txt` они не попадают. Кнопка «Синхронизировать» сверяет папку с префиксом и передаёт только изменившееся. Если что-то сломалось, достаточно прислать `logs.txt`.

Сборка EXE: workflow [Build Windows EXE](.github/workflows/build-windows.yml) прогоняет тесты и кладёт `sync.exe` в артефакт `sync-windows`.
