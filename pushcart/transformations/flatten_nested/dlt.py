# pylint: disable=import-error
# pyright: reportMissingImports=false
import dlt

import pushcart.transformations.flatten_nested.spark as ps


def flatten(source: str, target: str, streaming: bool = False) -> None:
    @dlt.table(name=target, temporary=True)
    def flatten_in_place(source_table: str = source):
        if streaming:
            df = dlt.read_stream(source_table)
        else:
            df = dlt.read(source_table)

        return ps.flatten(df)
