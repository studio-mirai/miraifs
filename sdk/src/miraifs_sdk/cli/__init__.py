import typer

from miraifs_sdk.cli import file, gas

app = typer.Typer()

app.add_typer(file.app, name="file")
app.add_typer(gas.app, name="gas")
