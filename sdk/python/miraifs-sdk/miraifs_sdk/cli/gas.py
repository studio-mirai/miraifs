import typer

from miraifs_sdk.sui import Sui

app = typer.Typer()


@app.command()
def merge():
    sui = Sui()
    gas_coins = sui.get_all_gas_coins(sui.config.active_address)
    result = sui.merge_coins(gas_coins)
    print(result)
    return


@app.command()
def split(
    quantity: int = typer.Argument(...),
    value: int = typer.Argument(...),
    denomination: str = typer.Option("sui"),
    auto_merge: bool = typer.Option(True),
):
    if denomination == "sui":
        typer.confirm(
            f"Please confirm you'd like to create {quantity}x {value} SUI coins.",
            abort=True,
        )
        value = value * 10**9

    sui = Sui()
    gas_coins = sui.get_all_gas_coins(sui.config.active_address)
    if len(gas_coins) > 1 and auto_merge:
        sui.merge_coins(gas_coins)
    result = sui.split_coin(gas_coins[0], quantity, value)
    print(result)
    return
