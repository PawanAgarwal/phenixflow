/**
 * Stock universes for the 200-week MA screener.
 *
 * S&P 500 and NASDAQ-100 constituent lists.
 * These lists change over time as indices rebalance; update periodically.
 */

// S&P 500 constituents (common tickers)
export const SP500 = [
  'AAPL','ABBV','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE',
  'AEP','AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALK',
  'ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN',
  'ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO',
  'ATVI','AVB','AVGO','AVY','AWK','AXP','AZO','BA','BAC','BAX',
  'BBWI','BBY','BDX','BEN','BF.B','BIO','BIIB','BK','BKNG','BKR',
  'BLK','BMY','BR','BRK.B','BRO','BSX','BWA','BXP','C','CAG',
  'CAH','CARR','CAT','CB','CBOE','CBRE','CCI','CCL','CDAY','CDNS',
  'CDW','CE','CEG','CF','CFG','CHD','CHRW','CHTR','CI','CINF',
  'CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP',
  'COF','COO','COP','COST','CPB','CPRT','CPT','CRL','CRM','CSCO',
  'CSGP','CSX','CTAS','CTLT','CTRA','CTSH','CTVA','CVS','CVX','CZR',
  'D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS',
  'DISH','DLR','DLTR','DOV','DOW','DPZ','DRI','DTE','DUK','DVA',
  'DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL',
  'EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ES','ESS',
  'ETN','ETR','ETSY','EVRG','EW','EXC','EXPD','EXPE','EXR','F',
  'FANG','FAST','FBHS','FCX','FDS','FDX','FE','FFIV','FIS','FISV',
  'FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GD',
  'GE','GEHC','GEN','GILD','GIS','GL','GLW','GM','GNRC','GOOG',
  'GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA',
  'HD','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY','HUM',
  'HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INTC','INTU',
  'INVH','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ',
  'J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KDP','KEY',
  'KEYS','KHC','KIM','KLAC','KMB','KMI','KMX','KO','KR','L',
  'LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNC','LNT',
  'LOW','LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA',
  'MAR','MAS','MCD','MCHP','MCK','MCO','MDLZ','MDT','MET','META',
  'MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST','MO','MOH',
  'MOS','MPC','MPWR','MRK','MRNA','MRO','MS','MSCI','MSFT','MSI',
  'MTB','MTCH','MTD','MU','NCLH','NDAQ','NDSN','NEE','NEM','NFLX',
  'NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA',
  'NVR','NWL','NWS','NWSA','NXPI','O','ODFL','OGN','OKE','OMC',
  'ON','ORCL','ORLY','OTIS','OXY','PARA','PAYC','PAYX','PCAR','PCG',
  'PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG',
  'PKI','PLD','PM','PNC','PNR','PNW','POOL','PPG','PPL','PRU',
  'PSA','PSX','PTC','PVH','PWR','PXD','PYPL','QCOM','QRVO','RCL',
  'RE','REG','REGN','RF','RHI','RJF','RL','RMD','ROK','ROL',
  'ROP','ROST','RSG','RTX','RVTY','SBAC','SBNY','SBUX','SCHW','SEE',
  'SHW','SIVB','SJM','SLB','SNA','SNPS','SO','SPG','SPGI','SRE',
  'STE','STLD','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY',
  'T','TAP','TDG','TDY','TECH','TEL','TER','TFC','TFX','TGT',
  'TJX','TMO','TMUS','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA',
  'TSN','TT','TTWO','TXN','TXT','TYL','UAL','UDR','UHS','ULTA',
  'UNH','UNP','UPS','URI','USB','V','VFC','VICI','VLO','VMC',
  'VRSK','VRSN','VRTX','VTR','VTRS','VZ','WAB','WAT','WBA','WBD',
  'WDC','WEC','WELL','WFC','WHR','WM','WMB','WMT','WRB','WRK',
  'WST','WTW','WY','WYNN','XEL','XOM','XRAY','XYL','YUM','ZBH',
  'ZBRA','ZION','ZTS'
];

// NASDAQ-100 constituents
export const NASDAQ100 = [
  'AAPL','ABNB','ADBE','ADI','ADP','ADSK','AEP','AMAT','AMD','AMGN',
  'AMZN','ANSS','ARM','ASML','AVGO','AZN','BIIB','BKNG','BKR','CCEP',
  'CDNS','CDW','CEG','CHTR','CMCSA','COST','CPRT','CRWD','CSCO','CSGP',
  'CSX','CTAS','CTSH','DASH','DDOG','DLTR','DXCM','EA','EXC','FANG',
  'FAST','FTNT','GEHC','GFS','GILD','GOOG','GOOGL','HON','IDXX','ILMN',
  'INTC','INTU','ISRG','KDP','KHC','KLAC','LRCX','LULU','MAR','MCHP',
  'MDB','MDLZ','MELI','META','MNST','MRNA','MRVL','MSFT','MU','NFLX',
  'NVDA','NXPI','ODFL','ON','ORLY','PANW','PAYX','PCAR','PDD','PEP',
  'PYPL','QCOM','REGN','RIVN','ROST','SBUX','SMCI','SNPS','SPLK','TEAM',
  'TMUS','TSLA','TTD','TTWO','TXN','VRSK','VRTX','WBD','WDAY','XEL','ZS'
];

// Additional high-quality blue chips not in the above
export const QUALITY_EXTRAS = [
  'BRK.B','V','MA','UNH','JNJ','PG','HD','LLY','MRK','PEP',
  'KO','ABBV','TMO','ACN','MCD','LIN','DHR','TXN','NEE','UNP',
  'PM','RTX','LOW','SPGI','SYK','ELV','ADP','ISRG','MDLZ','CI',
  'MMC','GD','ITW','APH','ECL','SHW','CTAS','MSCI','NDAQ','MCO'
];

/**
 * Return de-duplicated ticker list for a given universe.
 */
export function getUniverse(name) {
  let tickers;
  switch (name) {
    case 'sp500':
      tickers = SP500;
      break;
    case 'nasdaq100':
      tickers = NASDAQ100;
      break;
    case 'all':
      tickers = [...SP500, ...NASDAQ100, ...QUALITY_EXTRAS];
      break;
    default:
      tickers = [...SP500, ...NASDAQ100, ...QUALITY_EXTRAS];
      break;
  }
  return [...new Set(tickers)];
}
