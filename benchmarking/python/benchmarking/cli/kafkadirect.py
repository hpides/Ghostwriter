import click


@click.group()
@click.option("-m/--mode", type=click.Choice(["exclusive", "concurrent"]))
@click.pass_context
def kafkadirect():
    pass

@kafkadirect.group()
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