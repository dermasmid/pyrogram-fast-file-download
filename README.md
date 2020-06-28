# pyrogram-fast-file-download
Download files from telegram via pyrogram with threading

# The purpose of the file
[Pyrogram](https://github.com/pyrogram/pyrogram) is a great tool for interacting with the telegram api,
but downloading file with pyrogram is slow.
so i make this small file the downloads a given file in a few threads so the download is much faster.

# Usage
`from fast_download import FastDownload
FastDownload(peer-id = #chat_id, message_id = #message_id, parts = #how many threads to make, app = # the pyrogram client, file_name = a chosen file name)`
