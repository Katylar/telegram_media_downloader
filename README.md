Modified version of <a href="https://github.com/Dineshkarthik/telegram_media_downloader/">Dineshkarthik's Telegram Media Downloader</a>

Changes:

1. Renames files by prefixing the message_id. This prevents overwrites and errors due to special characters in the filename, overlong filenames, and duplicate filenames etc.

2. Saves the downloaded media into their own separate subfolders, with category subfolders inside. Config.yaml is saved there as well.

3. Terminal logs now only show the most important info.

4. Before files are marked sa successfully downloaded, their file-sizes are checked if they match the expected size. If they don't, the file is considered a failed_id and added to the retry list.

5. Total number of files are calculated at the start so users have a general idea on their progress as they download.


