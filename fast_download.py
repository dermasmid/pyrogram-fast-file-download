from concurrent.futures import ThreadPoolExecutor
from pyrogram.api.functions.channels import GetMessages
from pyrogram.api.types import InputMessageID, InputDocumentFileLocation, document_attribute_filename
import pyrogram
from pyrogram.session import Session, Auth
from pyrogram.api import functions
from pyrogram.errors import AuthBytesInvalid
import time
import os
from datetime import datetime






class FastDownload:

    def __init__(self, peer_id: int, message_id: int, parts: int, app: pyrogram.Client, file_name: str):
        self.app = app
        message_data = app.send(GetMessages(channel= (app.resolve_peer(peer_id= peer_id)), id= [InputMessageID(id= message_id)]))
        self.document_data = InputDocumentFileLocation(
            id= message_data["messages"][0]["media"]["document"]["id"], 
            file_reference= message_data["messages"][0]["media"]["document"]["file_reference"], 
            access_hash= message_data["messages"][0]["media"]["document"]["access_hash"],
            thumb_size= ""
        )
        self.size = message_data["messages"][0]["media"]["document"]["size"]
        for attribute in message_data["messages"][0]["media"]["document"]["attributes"]:
            if type(attribute) == document_attribute_filename.DocumentAttributeFilename:
                self.file_name = attribute["file_name"]
                break
        else:
            self.file_name = file_name
        self.apple = self.change_dc(message_data["messages"][0]["media"]["document"]["dc_id"])
        self.parts = parts
        self.temp_folder = f"{os.getcwd()}/downloads/"
        self.check_if_temp_folder_exists()
        self.mb = self.size // (1024 * 1024)
        self.part_size_in_mbs = self.mb // parts
        self.part_size_in_bytes = self.part_size_in_mbs * (1024 * 1024)
        self.part_data = {}
        self.limit_prefix = 1048576
        self.b4 = datetime.now()
        self.done = 0
        self.calculate()


    def calculate(self):
        offset = 0
        stop = self.part_size_in_bytes
        for part in range(self.parts):
            self.part_data[part] = {"offset": offset, "stop": stop}
            offset = stop
            stop += self.part_size_in_bytes
            if part == (self.parts - 2):
                end = (self.size - (self.part_size_in_bytes * self.parts))
                stop += end
        self.iter_parts()


    def iter_parts(self):
        self.list_of_funcs = []
        self.func_data = {}
        with ThreadPoolExecutor(max_workers= self.parts) as executer:
            for part in self.part_data:
               executer.submit(self.download_part, part)
        print(datetime.now() - self.b4)
        self.combine_files()


    def download_part(self, part):
        self.func_data[part] = {}
        self.func_data[part]["file"] = open(f"{self.temp_folder}{part}.tmp", "wb")
        while True:
            try:
                self.func_data[part]["data"] = self.apple.send(GetFile(location= self.document_data, offset= self.part_data[part]["offset"], limit= self.limit_prefix))
                self.done += 1
            except FloodWait as e:
                print("encountered a floodwait, waiting...")
                time.sleep(e.x)
                print("done waiting")
                self.func_data[part]["data"] = self.apple.send(GetFile(location= self.document_data, offset= self.part_data[part]["offset"], limit= self.limit_prefix))
            if self.part_data[part]["offset"] >= self.part_data[part]["stop"]:
                self.func_data[part]["file"].close()
                break
            self.func_data[part]["file"].write(self.func_data[part]["data"].bytes)
            self.part_data[part]["offset"] += self.limit_prefix
        
    def combine_files(self):
        with open(f"{self.temp_folder}0.tmp", "ab") as main_file:
            for file_number in range(self.parts):
                if file_number != 0:
                    with open(f"{self.temp_folder}{file_number}.tmp", "rb") as part_file:
                        data = part_file.read()
                        main_file.write(data)
                        os.remove(f"{self.temp_folder}{file_number}.tmp")
        os.rename(f"{self.temp_folder}0.tmp", f"{self.temp_folder}{self.file_name}")





    def change_dc(self, dc_id: int):
        with self.app.media_sessions_lock:
            session = self.app.media_sessions.get(dc_id, None)

            if session is None:
                if dc_id != self.app.storage.dc_id():
                    session = Session(self.app, dc_id, Auth(self.app, dc_id).create(), is_media=True)
                    session.start()

                    for _ in range(3):
                        exported_auth = self.app.send(
                            functions.auth.ExportAuthorization(
                                dc_id=dc_id
                            )
                        )

                        try:
                            session.send(
                                functions.auth.ImportAuthorization(
                                    id=exported_auth.id,
                                    bytes=exported_auth.bytes
                                )
                            )
                        except AuthBytesInvalid:
                            continue
                        else:
                            break
                    else:
                        session.stop()
                        raise AuthBytesInvalid
                else:
                    session = Session(self.app, dc_id, self.app.storage.auth_key(), is_media=True)
                    session.start()

                self.app.media_sessions[dc_id] = session
        return session



    def check_if_temp_folder_exists(self):
        if not os.path.exists(self.temp_folder):
            os.mkdir(self.temp_folder)