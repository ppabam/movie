from movie.api.call import gen_url, call_api
import os

def test_gen_url_default():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r

def test_gen_url():
    r = gen_url(url_param={"multiMovieYn": "Y", "repNationCd": "K"})
    assert "&multiMovieYn=Y" in r
    assert "&repNationCd=K" in r

def test_call_api():
    r = call_api()
    assert isinstance(r, list)
    assert isinstance(r[0]['rnum'], str)
    assert len(r) == 10
    for e in r:
        assert isinstance(e, dict)

# def test_list2df():
#     data = call_api()
#     df = list2df(data)
#     assert isinstance(df, pd.DataFrame)
#     assert len(data) == len(df)
#     assert set(data[0].keys()).issubset(set(df.columns))

