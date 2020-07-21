import run_appsflyer_data_sync as app

#is_reattr = True
is_reattr = False

app.run_appsflyer_data_sync({ "is_reattr": is_reattr, "disable_bigquery": True }, {})