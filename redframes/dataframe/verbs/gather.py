import pandas as pd


def gather(df: pd.DataFrame, columns: list[str], into: tuple[str, str]) -> pd.DataFrame:
    index = [col for col in df.columns if col not in columns]
    df = pd.melt(
        df,
        id_vars=index,
        value_vars=columns,
        var_name=into[0],
        value_name=into[1],
    )
    df = df.dropna(subset=into[1]) # type: ignore
    return df
