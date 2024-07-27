from miraifs_sdk.models import GasCoin
from pysui import SuiConfig, SyncClient, handle_result
from pysui.sui.sui_builders.get_builders import GetCoins, GetObjectsOwnedByAddress
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_txresults.single_tx import (
    AddressOwner,
    ObjectRead,
    ObjectReadPage,
    SuiCoinObjects,
)
from pysui.sui.sui_types import ObjectID, SuiAddress


class Sui:
    def __init__(self) -> None:
        self.config = SuiConfig.default_config()
        self.client = SyncClient(self.config)

    def get_all_gas_coins(
        self,
        address: SuiAddress,
    ) -> list[GasCoin]:
        all_gas_coins: list[GasCoin] = []

        cursor = None
        while True:
            builder = GetCoins(
                owner=address,
                limit=50,
                cursor=cursor,
            )
            result = handle_result(self.client.execute(builder))

            if isinstance(result, SuiCoinObjects):
                gas_coins = [
                    GasCoin(
                        id=coin.coin_object_id,
                        balance=coin.balance,
                    )
                    for coin in result.data
                ]
                all_gas_coins.extend(gas_coins)

                # Break out of the loop if there are no more pages.
                if not result.next_cursor:
                    break

                cursor = ObjectID(result.next_cursor)
            else:
                break

        all_gas_coins.sort(key=lambda x: x.balance, reverse=True)
        return all_gas_coins

    def split_coin(
        self,
        coin: GasCoin,
        quantity: int,
        value: int,
    ) -> list[GasCoin]:
        """
        Split a coin into multiple coins of the specified value,
        and return a list of newly created GasCoin objects.

        Args:
            coin (GasCoin): The coin to split.
            quantity (int): The number of coins to split the coin into.
            value (int): The value of each coin.
        """
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        coins = txer.split_coin(
            coin=txer.gas,
            amounts=[value for _ in range(quantity)],
        )

        txer.transfer_objects(
            transfers=coins,
            recipient=self.config.active_address,
        )

        result = handle_result(
            txer.execute(
                use_gas_object=ObjectID(coin.id),
            )
        )

        if isinstance(result, TxResponse):
            coins: list[GasCoin] = []
            created_objs = result.effects.created
            for obj in created_objs:
                coin = GasCoin(
                    id=obj.reference.object_id,
                    balance=value,
                )
                coins.append(coin)

        return coins

    from rich import print

    def merge_coins(
        self,
        coins: list[GasCoin],
    ) -> GasCoin:
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        gas_coin = coins.pop(0)

        txer.merge_coins(
            merge_to=txer.gas,
            merge_from=[ObjectID(coin.id) for coin in coins],
        )

        result = handle_result(
            txer.execute(
                use_gas_object=ObjectID(gas_coin.id),
            ),
        )

        if isinstance(result, TxResponse):
            pass

        return gas_coin

    def get_owner_address(
        self,
        object_id: str,
    ) -> str:
        result = handle_result(
            self.client.get_object(object_id),
        )

        if isinstance(result, ObjectRead):
            if isinstance(result.owner, AddressOwner):
                return result.owner.address_owner

    def get_owned_objects(
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
    ) -> list[ObjectRead]:
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
            result = handle_result(self.client.execute(builder))
            if isinstance(result, ObjectReadPage):
                all_objects.extend(result.data)
            if result.has_next_page is True:
                cursor = result.next_cursor
            else:
                break

        return all_objects

    def find_largest_gas_coin(
        self,
        coins: list[GasCoin],
    ):
        return max(coins, key=lambda coin: coin.balance)
