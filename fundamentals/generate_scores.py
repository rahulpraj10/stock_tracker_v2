import pandas as pd
import numpy as np


def trend_score(series):
    series = np.array(series)

    if len(series) < 3:
        return 50

    x = np.arange(len(series))
    slope = np.polyfit(x, series, 1)[0]
    volatility = np.std(series)
    score = 50

    if slope > 0:
        score += 30
    else:
        score -= 30

    score -= volatility * 5
    return max(0, min(100, score))

def revenue_trend(qoq_df):

    rev_growth = qoq_df["rev_yoy"].tail(6)
    return trend_score(rev_growth)


def profit_trend(qoq_df):

    profit_growth = qoq_df["profit_yoy"].tail(6)
    return trend_score(profit_growth)

def roe_trend(roe_df):

    roe = roe_df.iloc[:,1].tail(5)
    return trend_score(roe)

def debt_trend(bs_df):

    debt = bs_df["debt_to_equity"].tail(5)
    # inverse trend because lower debt is good
    return trend_score(-debt)

def smart_money(sh_df):

    fii = sh_df["fii_holding"].tail(4)
    dii = sh_df["dii_holding"].tail(4)

    combined = fii + dii

    return trend_score(combined)

def company_score(company):

    qoq = pd.DataFrame(company["qoq_features"])
    bs = pd.DataFrame(company["bs_features"])
    roe = pd.DataFrame(company["roe_years_df"])
    sh = pd.DataFrame(company["sh_features"])

    scores = {
        "RevenueTrend": revenue_trend(qoq),
        "ProfitTrend": profit_trend(qoq),
        "ROETrend": roe_trend(roe),
        "DebtTrend": debt_trend(bs),
        "SmartMoney": smart_money(sh)
    }

    final = np.mean(list(scores.values()))

    scores["FinalScore"] = final

    return scores



def create_score(SC):
    link = f"https://www.screener.in/company/{SC}/"
    a = pd.read_html(link)

    pl_qtr_raw = a[0]
    # Set row names as index
    pl = pl_qtr_raw.set_index("Unnamed: 0")

    # Transpose → time-series format
    pl = pl.T
    pl.index = pd.to_datetime(pl.index, errors="coerce", format='%b %Y')

    # Convert numeric columns
    for col in pl.columns:
        pl[col] = (
            pl[col]
            .astype(str)
            .str.replace("%", "")
            .replace("NaN", np.nan)
            .astype(float)
        )
    features = pd.DataFrame(index=pl.index)

    features["revenue"] = pl.iloc[:, [
        [index for index, item in enumerate(pl.columns) if 'Revenue' in item] + [index for index, item in
                                                                                 enumerate(pl.columns) if
                                                                                 'Sales' in item]][
        0]]  # pl["Revenue +"]
    features["operating_profit"] = pl.iloc[
        :, [index for index, item in enumerate(pl.columns) if 'Operating Profit' in item][0]]  # pl["Net Profit +"]
    features["net_profit"] = pl.iloc[
        :, [index for index, item in enumerate(pl.columns) if 'Net Profit' in item][0]]  # pl["Net Profit +"]

    # Growth
    features["rev_qoq"] = features["revenue"].pct_change()
    features["rev_yoy"] = features["revenue"].pct_change(4)

    features["profit_qoq"] = features["net_profit"].pct_change()
    features["profit_yoy"] = features["net_profit"].pct_change(4)

    # Margins
    features["profit_margin"] = (
            features["net_profit"] / features["revenue"]
    )

    bs_raw = a[6]
    bs = bs_raw.set_index("Unnamed: 0").T
    bs.index = pd.to_datetime(bs.index, errors="coerce", format='%b %Y')

    bs = bs.astype(float)

    bs_features = pd.DataFrame(index=bs.index)
    bs_features["Borrowing"] = bs.iloc[
        :, [index for index, item in enumerate(bs.columns) if 'Borrowing' in item][0]]  # pl["Revenue +"]

    bs_features["debt_to_equity"] = (
            bs_features["Borrowing"] / (bs["Equity Capital"] + bs["Reserves"])
    )

    bs_features["asset_growth"] = bs["Total Assets"].pct_change()
    bs_features["Total Assets"] = bs["Total Assets"]
    #bs_features

    cf_raw = a[7]
    cf = cf_raw.set_index("Unnamed: 0").T
    cf.index = pd.to_datetime(cf.index, errors="coerce", format='%b %Y')
    cf = cf.astype(float)

    cf_features = pd.DataFrame(index=cf.index)

    cf_features["operating_cf"] = cf.iloc[
        :, [index for index, item in enumerate(cf.columns) if 'Cash from Operating Activity' in item][
            0]]  # cf["Cash from Operating Activity +"]
    cf_features["cf_to_profit"] = (
            cf_features["operating_cf"] /
            features["net_profit"].reindex(cf.index)
    )
    #cf_features

    roe_raw = a[8]
    roe = roe_raw.set_index("Unnamed: 0").T
    roe.index = pd.to_datetime(roe.index, errors="coerce", format='%b %Y')
    # Could contain either ROE or ROCE. We will assume it to be same and process under ROE
    pattern = 'ROE'
    matching_columns = roe.filter(like=pattern, axis=1).columns
    if not matching_columns.empty:
        roe['ROE %'] = roe['ROE %'].replace('%', '', regex=True).astype(float)
    else:
        print('Here')
        roe['ROE %'] = roe['ROCE %'].replace('%', '', regex=True).astype(float)
        roe['ROCE %'] = roe['ROCE %'].replace('%', '', regex=True).astype(float)
        # roe.drop("ROCE %", inplace = True)
    roe = roe.astype(float)

    roe_features = pd.DataFrame(index=roe.index)
    roe_features["roe"] = roe["ROE %"]
    roe_features["roe_trend"] = roe_features["roe"].diff()
    #roe_features

    shareholding_raw = a[11]
    sh = shareholding_raw.set_index("Unnamed: 0").T
    sh.index = pd.to_datetime(sh.index, errors="coerce", format='%b %Y')

    sh["Promoters"] = sh.iloc[
        :, [index for index, item in enumerate(sh.columns) if 'Promoters' in item][0]].str.replace("%", "").astype(
        float)
    sh["fii_holding"] = sh.iloc[:, [index for index, item in enumerate(sh.columns) if 'FII' in item][0]].str.replace(
        "%", "").astype(float)
    sh["dii_holding"] = sh.iloc[:, [index for index, item in enumerate(sh.columns) if 'DII' in item][0]].str.replace(
        "%", "").astype(float)

    sh_features = pd.DataFrame(index=sh.index)
    sh_features["promoter_holding"] = sh["Promoters"]
    sh_features["promoter_change"] = sh_features["promoter_holding"].diff()

    sh_features["fii_holding"] = sh["fii_holding"]
    sh_features["dii_holding"] = sh["dii_holding"]

    qoq_features = features.reset_index()
    qoq_features.rename(columns={'index': 'Date'}, inplace=True)

    bs_features1 = bs_features.reset_index()
    bs_features1.rename(columns={'index': 'Date'}, inplace=True)

    cf_features1 = cf_features.reset_index()
    cf_features1.rename(columns={'index': 'Date'}, inplace=True)
    #cf_features1

    roe_features1 = roe_features.reset_index()
    roe_features1.rename(columns={'index': 'Date'}, inplace=True)
    #roe_features1

    compounded_sales_growth_df = a[2]
    compounded_profit_growth_df = a[3]
    stock_price_CAGR_df = a[4]
    ROE_years_df = a[5]

    sh_features1 = sh_features.reset_index()
    sh_features1.rename(columns={'index': 'Date'}, inplace=True)
    #sh_features1

    ROE_years_df.rename(columns={'Return on Equity': 'year', 'Return on Equity.1': 'return_on_equity'}, inplace=True)

    ROE_years_df["return_on_equity"] = ROE_years_df.iloc[
        :, [index for index, item in enumerate(ROE_years_df.columns) if 'return_on_equity' in item][0]].str.replace("%",
                                                                                                                    "").astype(
        float)

    # Make them as dict
    fundamentals_dict = {
        'qoq_features': qoq_features.to_dict(orient='records'),
        'bs_features': bs_features1.to_dict(orient='records'),
        'cf_features': cf_features1.to_dict(orient='records'),
        'roe_features': roe_features1.to_dict(orient='records'),
        'compounded_sales_growth_df': compounded_sales_growth_df.to_dict(orient='records'),
        'compounded_profit_growth_df': compounded_profit_growth_df.to_dict(orient='records'),
        'stock_price_CAGR_df': stock_price_CAGR_df.to_dict(orient='records'),
        'roe_years_df': ROE_years_df.to_dict(orient='records'),
        'sh_features': sh_features1.to_dict(orient='records')
    }

    company_fundamentals_dict = {SC: fundamentals_dict}

    score = company_score(fundamentals_dict)['FinalScore']
    print(f'Scores generated: {score} for SC: {SC}')

    return score, company_fundamentals_dict


