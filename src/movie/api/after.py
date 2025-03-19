import pandas as pd


def fillna_meta(previous_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    # movieCd 기준으로 outer join
    merged_df = A.merge(B, on="movieCd", how="outer", suffixes=("_A", "_B"))
    print(merged_df)

    # NaN 값 대체 (A 값 우선, 없으면 B 값 사용)
    merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(
        merged_df["multiMovieYn_B"]
    )
    merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(
        merged_df["repNationCd_B"]
    )

    # 불필요한 컬럼 제거
    merged_df = merged_df[["movieCd", "multiMovieYn", "repNationCd"]]
