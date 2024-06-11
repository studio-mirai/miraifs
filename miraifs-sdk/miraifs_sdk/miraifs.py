from miraifs_sdk.sui import Sui
from miraifs_sdk import PACKAGE_ID


class MiraiFs(Sui):
    def __init__(
        self,
    ) -> None:
        super().__init__()
        self.package_id = PACKAGE_ID
