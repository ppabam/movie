import pandas as pd
from movie.api.after import fillna_meta


def test_fillna_meta():
    A = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "Y", "N"],
            "repNationCd": ["K", "F", None],
        }
    )

    B = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(A, B)

    assert True
