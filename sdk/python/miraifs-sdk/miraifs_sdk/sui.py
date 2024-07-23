from miraifs_sdk.utils import to_mist
from pydantic import BaseModel
from pysui import SuiConfig, SyncClient, handle_result
from pysui.sui.sui_builders.get_builders import GetCoins, GetObjectsOwnedByAddress
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_txresults.single_tx import AddressOwner, ObjectRead, SuiCoinObjects
from pysui.sui.sui_types import ObjectID, SuiAddress


class GasCoin(BaseModel):
    id: str
    balance: int


class Sui:
    def __init__(self) -> None:
        self.config = SuiConfig.default_config()
        self.client = SyncClient(self.config)

    def get_all_gas_coins(
        self,
    ) -> list[GasCoin]:
        all_gas_coins: list[GasCoin] = []

        cursor = None
        while True:
            builder = GetCoins(
                owner=self.config.active_address,
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
            coin=ObjectID(coin.id),
            amounts=[to_mist(value) for _ in range(quantity)],
        )

        txer.transfer_objects(
            transfers=coins,
            recipient=self.config.active_address,
        )

        result = handle_result(txer.execute())

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

    def merge_coins(
        self,
        coins: list[GasCoin],
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        gas_coin = coins.pop(0)

        coin_to_merge_to = txer.move_call(
            target="0x2::coin::zero",
            arguments=[],
            type_arguments=["0x2::sui::SUI"],
        )

        txer.merge_coins(
            merge_to=coin_to_merge_to,
            merge_from=[ObjectID(coin.id) for coin in coins],
        )

        txer.transfer_objects(
            transfers=[coin_to_merge_to],
            recipient=self.config.active_address,
        )

        result = handle_result(
            txer.execute(
                use_gas_object=ObjectID(gas_coin.id),
            ),
        )

        return result

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
            result = self.client.execute(builder)

            if result.is_ok():
                objects = result.result_data.data
                if len(objects) > 0:
                    all_objects.extend(objects)

            if result.result_data.has_next_page is True:
                cursor = result.result_data.next_cursor
            else:
                break

        return all_objects

    def find_largest_gas_coin(
        self,
        coins: list[GasCoin],
    ):
        return max(coins, key=lambda coin: coin.balance)
