import click


@click.group()
@click.option("-m/--mode", type=click.Choice(["exclusive", "concurrent"]))
@click.pass_context
def ghostwriter():
    pass

@ghostwriter.group()
def ysb():
    pass

@ysb.command()
def sustainable():
    ...

@ghostwriter.group()
def micro():
    pass

@micro.command()
def producer():
    ...

@micro.command()
def consumer():
    ...

@micro.command()
def sustainable():
    ...

@micro.command()
def roundtrip():
    ...