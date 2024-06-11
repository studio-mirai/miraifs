from pydantic import BaseModel
from pysui import AsyncClient, SuiConfig, handle_result
from pysui.sui.sui_builders.get_builders import (
    GetCoins,
    GetObjectsOwnedByAddress,
)
from pysui.sui.sui_txresults.single_tx import (
    AddressOwner,
    ObjectRead,
    SuiCoinObjects,
)
from pysui.sui.sui_types import SuiAddress


class GasCoin(BaseModel):
    id: str
    balance: int


class Sui:
    def __init__(self) -> None:
        self.config = SuiConfig.default_config()
        self.client = AsyncClient(self.config)

    async def get_gas_coins(
        self,
        count: int,
    ) -> list[GasCoin]:
        builder = GetCoins(
            owner=self.config.active_address,
            limit=count,
        )
        result = handle_result(await self.client.execute(builder))

        if isinstance(result, SuiCoinObjects):
            return [
                GasCoin(
                    id=coin.coin_object_id,
                    balance=coin.balance,
                )
                for coin in result.data
            ]

    async def get_owner_address(
        self,
        object_id: str,
    ) -> str:
        result = handle_result(
            await self.client.get_object(object_id),
        )

        if isinstance(result, ObjectRead):
            if isinstance(result.owner, AddressOwner):
                return result.owner.address_owner

    async def get_owned_objects(
        self,
        address: str,
        struct_type: str,
        show_type: bool = False,
        show_owner: bool = False,
        show_previous_transaction: bool = False,
        show_display: bool = False,
        show_content: bool = False,
        show_bcs: bool = False,
        show_storage_rebate: bool = False,
    ) -> list[str]:
        filter = {
            "filter": {
                "StructType": struct_type,
            },
            "options": {
                "showType": show_type,
                "showOwner": show_owner,
                "showPreviousTransaction": show_previous_transaction,
                "showDisplay": show_display,
                "showContent": show_content,
                "showBcs": show_bcs,
                "showStorageRebate": show_storage_rebate,
            },
        }

        all_objects = []

        cursor = None
        while True:
            builder = GetObjectsOwnedByAddress(
                SuiAddress(address),
                query=filter,
                cursor=cursor,
                limit=50,
            )
            result = await self.client.execute(builder)

            if result.is_ok():
                objects = result.result_data.data
                if len(objects) > 0:
                    all_objects.extend(objects)

            if result.result_data.has_next_page is True:
                cursor = result.result_data.next_cursor
            else:
                break

        return all_objects
