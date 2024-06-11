from miraifs_sdk.sui import Sui
from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.miraifs.file import File


class MiraiFs(Sui):
    def __init__(
        self,
    ) -> None:
        super().__init__()
        self.package_id = PACKAGE_ID

    def File(
        self,
        file_id: str,
    ) -> File:
        return File(
            file_id=file_id,
        )
