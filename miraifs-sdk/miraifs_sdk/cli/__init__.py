import typer
from miraifs_sdk.cli import file


app = typer.Typer()

app.add_typer(file.app, name="file")
