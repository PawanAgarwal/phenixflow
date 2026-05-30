#!/usr/bin/env python3
"""
Build a systematic, comprehensive investable-asset taxonomy and write it to
an Excel workbook (multi-sheet) plus a flat master CSV.

Hierarchy columns:
  Tier            -> broad grouping bucket (e.g. "Credit / Fixed Income")
  Asset Class     -> top-level class
  Sub-Class       -> the next level down
  Sector/Strategy -> sector, strategy, or structural slice
  Sub-Sector      -> finest grain (industry, tranche, sub-strategy, region)
  Geography       -> Domestic (US) / Developed ex-US / EM / Frontier / Global
  Examples        -> representative tickers / instruments (illustrative, not advice)
  Typ. Yield      -> rough income range where meaningful
  Risk Tier       -> 1 (capital-preservation) .. 6 (max beta / first loss)
  Liquidity       -> Daily / Intraday / Periodic / Illiquid
  Notes           -> what you're really betting on / key risk

Yields and risk tiers are rough, illustrative ranges as of generation date,
NOT investment advice. Tickers are examples to anchor the category, not
recommendations.
"""

import csv
import os
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

COLUMNS = [
    "Tier", "Asset Class", "Sub-Class", "Sector/Strategy", "Sub-Sector",
    "Geography", "Examples", "Typ. Yield", "Risk Tier", "Liquidity", "Notes",
]

# Each row: (Tier, AssetClass, SubClass, Sector, SubSector, Geo, Examples, Yield, Risk, Liq, Notes)
ROWS = []
def R(*args):
    ROWS.append(args)

# =====================================================================
# TIER 0 — CASH & CASH EQUIVALENTS
# =====================================================================
T = "Cash & Equivalents"
R(T,"Cash","Bank deposits","Checking/Savings","FDIC savings","Domestic","HYSA","4-5%",1,"Daily","Insured to limits; reinvestment risk")
R(T,"Cash","Bank deposits","Certificates of deposit","Brokered CDs","Domestic","-","4-5%",1,"Periodic","Term lockup; FDIC insured")
R(T,"Cash","Money market","Govt money market funds","T-bill/repo MMF","Domestic","SGOV,BIL,SPAXX","4-5%",1,"Daily","Near-zero credit risk; rate-sensitive income")
R(T,"Cash","Money market","Prime money market funds","CP/CD MMF","Domestic","-","4-5%",1,"Daily","Slight credit spread over govt")
R(T,"Cash","T-bills","Ultra-short Treasuries","0-3m bills","Domestic","BIL,SGOV","4-5%",1,"Intraday","Risk-free rate proxy")
R(T,"Cash","Stablecoins","Fiat-backed","USD stablecoins","Global","USDC,USDT","0-5%*",2,"Intraday","Peg/issuer risk; *yield via lending")

# =====================================================================
# TIER 1 — GOVERNMENT FIXED INCOME
# =====================================================================
T = "Govt Fixed Income"
R(T,"US Treasuries","Nominal","Short duration","1-3y","Domestic","SHY,VGSH","4-5%",1,"Intraday","Duration/rate risk only")
R(T,"US Treasuries","Nominal","Intermediate","3-10y","Domestic","IEF,VGIT","4%",2,"Intraday","Belly of the curve")
R(T,"US Treasuries","Nominal","Long duration","10-30y","Domestic","TLT,VGLT,EDV","4-5%",3,"Intraday","High rate convexity; big drawdowns")
R(T,"US Treasuries","Inflation-linked","TIPS","Short/Long TIPS","Domestic","TIP,VTIP,SCHP,LTPZ","2%+CPI",2,"Intraday","Bets on realized inflation")
R(T,"US Treasuries","Floating rate","FRN","2y FRN","Domestic","USFR,TFLO","4-5%",1,"Intraday","Minimal duration; tracks bills")
R(T,"US Treasuries","STRIPS","Zero coupon","Long zeros","Domestic","GOVZ,EDV","4-5%",4,"Intraday","Max duration convexity")
R(T,"Agency","GSE debt","Agency debentures","FNMA/FHLB","Domestic","-","4-5%",1,"Daily","Implicit govt backing")
R(T,"Sovereign ex-US","Developed","DM govt bonds","Bunds/JGBs/Gilts","Developed ex-US","BWX,IGOV","1-4%",2,"Daily","FX + foreign rate risk")
R(T,"Sovereign ex-US","Developed FX-hedged","Hedged DM govt","Hedged global agg","Developed ex-US","BNDX","2-3%",2,"Daily","Strips FX, keeps rate diversification")
R(T,"Sovereign ex-US","EM hard ccy","USD sovereign","EM USD bonds","EM","EMB,PCY","6-8%",4,"Daily","Spread + default risk, no FX")
R(T,"Sovereign ex-US","EM local ccy","Local sovereign","EM local bonds","EM","EMLC,LEMB","6-8%",4,"Daily","FX + local rate + default")
R(T,"Sovereign ex-US","Frontier","Frontier sovereign","Frontier USD bonds","Frontier","FMB*","8-12%",5,"Periodic","Illiquid, high default/political risk")

# =====================================================================
# TIER 2 — MUNICIPAL
# =====================================================================
T = "Municipal"
R(T,"Munis","General obligation","Investment-grade GO","National GO","Domestic","MUB,VTEB","3-4%TE",2,"Daily","Tax-exempt; state credit risk")
R(T,"Munis","Revenue","IG revenue","Toll/utility/hospital","Domestic","-","3-4%TE",2,"Daily","Project cash-flow backed")
R(T,"Munis","High yield","HY munis","Non-rated/spec muni","Domestic","HYD,HYMB","4-5%TE",3,"Daily","Lower-rated issuers; default risk")
R(T,"Munis","State-specific","Single-state","CA/NY munis","Domestic","-","3-4%TE",2,"Daily","Double tax-exempt for residents")
R(T,"Munis","Taxable munis","BABs","Build America Bonds","Domestic","BAB","4-5%",2,"Daily","Taxable; for non-taxable accounts")
R(T,"Munis","Pre-refunded","Escrowed","Pre-re munis","Domestic","-","3%TE",1,"Daily","Treasury-collateralized; very safe")

# =====================================================================
# TIER 3 — CORPORATE CREDIT
# =====================================================================
T = "Corporate Credit"
R(T,"Investment grade","Broad IG","Aggregate corp","US IG corp","Domestic","LQD,VCIT","4-5%",2,"Intraday","Spread + rate risk")
R(T,"Investment grade","Short IG","1-5y IG","Short corp","Domestic","IGSB,VCSH","4-5%",2,"Intraday","Low duration credit")
R(T,"Investment grade","Long IG","10y+ IG","Long corp","Domestic","IGLB,VCLT","5%",3,"Intraday","Duration + spread")
R(T,"Investment grade","Crossover","BBB","BBB-tilt","Domestic","-","5%",3,"Intraday","Fallen-angel risk")
R(T,"High yield","Broad HY","BB/B/CCC","US HY","Domestic","HYG,JNK,USHY","6-8%",4,"Intraday","Default + spread risk")
R(T,"High yield","Fallen angels","Ex-IG downgrades","Fallen angel HY","Domestic","FALN","6-7%",4,"Daily","Higher quality HY tilt")
R(T,"High yield","Short HY","0-5y HY","Short HY","Domestic","SHYG,SJNK","6-7%",4,"Intraday","Lower duration HY")
R(T,"High yield","Distressed","CCC/defaulted","Distressed debt","Domestic","-","10%+",6,"Illiquid","Workout/recovery bet")
R(T,"Convertibles","Convertible bonds","Balanced converts","US converts","Domestic","CWB,ICVT","2-4%",4,"Daily","Equity upside + bond floor")
R(T,"International corp","DM corporate","IG/HY foreign","Euro/UK corp","Developed ex-US","IBND,HYXU","3-6%",3,"Daily","FX + foreign credit")
R(T,"International corp","EM corporate","EM USD corp","EM corp bonds","EM","CEMB,EMCB","6-8%",4,"Daily","EM corporate credit")

# =====================================================================
# TIER 4 — SECURITIZED / STRUCTURED CREDIT  (the CLO ladder lives here)
# =====================================================================
T = "Securitized Credit"
R(T,"Agency MBS","Pass-throughs","30y/15y MBS","Agency RMBS","Domestic","MBB,VMBS","4-5%",2,"Intraday","Prepayment/convexity risk")
R(T,"Non-agency MBS","RMBS","Prime/Alt-A/legacy","Non-agency RMBS","Domestic","-","5-7%",4,"Periodic","Housing credit risk")
R(T,"CMBS","Commercial MBS","Conduit/SASB","CMBS senior","Domestic","CMBS*","5-7%",4,"Daily","CRE credit risk")
R(T,"ABS","Consumer ABS","Auto/card/student","Consumer ABS","Domestic","-","5-6%",3,"Daily","Consumer credit cycle")
R(T,"ABS","Esoteric ABS","Aircraft/royalty/data","Esoteric ABS","Domestic","-","6-8%",4,"Periodic","Niche collateral risk")
R(T,"CLO","CLO debt","AAA tranche","Top tranche","Domestic","JAAA,AAA,CLOA","5-6%",1,"Daily","Spread normalize; near-zero default")
R(T,"CLO","CLO debt","AA/A tranche","Senior mezz","Domestic","JAAA-adj*","6%",2,"Daily","Slightly subordinated, floating")
R(T,"CLO","CLO debt","BBB tranche","Mezz","Domestic","JBBB,CLOZ","6-7%",2,"Daily","Spread tightening + floating rate")
R(T,"CLO","CLO debt","BB tranche","Lower mezz","Domestic","CLOZ,BBB*","8-9%",4,"Daily","First-loss-adjacent; higher beta")
R(T,"CLO","CLO equity","Residual/equity","First-loss","Domestic","ECC,OXLC","15-20%+",6,"Daily","Everything-goes-right residual")
R(T,"Multi-sector","Active securitized","Go-anywhere structured","Multi-sector credit","Domestic","-","6-8%",4,"Daily","Manager rotates structured sleeves")

# =====================================================================
# TIER 5 — BANK / SENIOR LOANS
# =====================================================================
T = "Loans"
R(T,"Senior loans","Leveraged loans","Broadly syndicated","Senior secured","Domestic","BKLN,SRLN","8%",4,"Daily","Floating; single-name default risk")
R(T,"Senior loans","Active loans","CLO-style mgmt","Bank loan funds","Domestic","FFRHX,SRLN","8%",4,"Daily","No structural leverage")
R(T,"Senior loans","Middle-market loans","Direct loans","MM senior","Domestic","-","9-11%",5,"Periodic","Smaller borrowers, less liquid")

# =====================================================================
# TIER 6 — PREFERRED & HYBRID CAPITAL
# =====================================================================
T = "Preferred & Hybrid"
R(T,"Preferreds","Traditional preferreds","$25 par perpetual","Bank/utility pfd","Domestic","PFF,PGX","6-7%",3,"Daily","Rate + credit; below senior debt")
R(T,"Preferreds","Institutional preferreds","$1000 par","Inst'l pfd","Domestic","PFFA,PFFD","6-7%",3,"Daily","Higher quality issuers")
R(T,"Preferreds","CLO-CEF preferreds","Term preferred","Senior to CEF common","Domestic","OXLCN,OXLCI,OXLCZ,ECC-D","7-8.5%",3,"Daily","Redeemed at $25 ahead of equity")
R(T,"Baby bonds","Exchange-traded debt","$25 baby bonds","Senior to pfd","Domestic","-","7-8%",3,"Daily","Bond claim, exchange-traded")
R(T,"CoCos","Contingent convertibles","AT1 bank capital","CoCo bonds","Developed ex-US","-","7-9%",5,"Daily","Write-down/conversion trigger risk")
R(T,"Convertible pfd","Convertible preferreds","Equity-linked pfd","Convert pfd","Domestic","-","5-7%",4,"Daily","Hybrid equity upside")

# =====================================================================
# TIER 7 — PRIVATE CREDIT / DIRECT LENDING
# =====================================================================
T = "Private Credit"
R(T,"BDC","Listed BDC","Top-tier direct lender","Senior direct lending","Domestic","ARCC,MAIN,OBDC","9-10%",5,"Daily","Direct-lending equity; soft marks")
R(T,"BDC","BDC basket","Diversified BDC","BDC index","Domestic","BIZD,PBDC","9-11%",5,"Daily","Private-credit beta")
R(T,"BDC","Venture/specialty BDC","Venture debt","Tech lending BDC","Domestic","HTGC,TRIN","10-12%",5,"Daily","Higher-risk borrowers")
R(T,"Interval funds","Private credit interval","Non-traded","Direct lending interval","Domestic","CCLFX*","8-10%",5,"Periodic","Quarterly liquidity gates")
R(T,"Private debt funds","LP direct lending","Drawdown funds","Senior/unitranche","Domestic","-","9-12%",5,"Illiquid","Lockup; J-curve")
R(T,"Mezzanine","Mezz/PIK debt","Subordinated","Mezzanine","Domestic","-","11-14%",6,"Illiquid","Below senior; equity kickers")

# =====================================================================
# TIER 8 — US EQUITIES (size / style / sector / industry)
# =====================================================================
T = "US Equities"
# Broad & size
R(T,"Broad market","Total market","Cap-weighted","US total","Domestic","VTI,ITOT","1-2%",4,"Intraday","US equity beta")
R(T,"Broad market","Large cap","S&P 500","Large blend","Domestic","SPY,VOO,IVV","1-2%",4,"Intraday","Mega/large cap beta")
R(T,"Broad market","Mid cap","S&P 400","Mid blend","Domestic","IJH,VO","1-2%",4,"Intraday","Mid-cap premium")
R(T,"Broad market","Small cap","Russell 2000","Small blend","Domestic","IWM,VB,IJR","1%",5,"Intraday","Small-cap/economic beta")
R(T,"Broad market","Micro cap","Micro-cap index","Micro","Domestic","IWC","1%",5,"Daily","Illiquidity premium")
# Style
R(T,"Style","Growth","Large growth","Growth tilt","Domestic","VUG,IWF,QQQ","0-1%",4,"Intraday","Duration/long-growth bet")
R(T,"Style","Value","Large value","Value tilt","Domestic","VTV,IWD","2-3%",4,"Intraday","Cheapness factor")
R(T,"Style","Dividend growth","Quality dividends","Div growers","Domestic","SCHD,VIG,DGRO","2-3%",3,"Intraday","Quality income compounding")
R(T,"Style","High dividend","Yield tilt","High div","Domestic","HDV,SPYD,VYM","3-4%",3,"Intraday","Income-tilted equity")
R(T,"Factor","Quality","High ROE/low debt","Quality factor","Domestic","QUAL","1-2%",4,"Intraday","Profitability factor")
R(T,"Factor","Momentum","Trend","Momentum factor","Domestic","MTUM","1%",4,"Intraday","Price persistence")
R(T,"Factor","Low volatility","Min vol","Low-vol factor","Domestic","USMV,SPLV","2%",3,"Intraday","Defensive factor")
R(T,"Factor","Multi-factor","Combined","Multi-factor","Domestic","LRGF,GSLC","1-2%",4,"Intraday","Diversified factor exposure")
# GICS sectors -> sub-industries
R(T,"Sector","Information Technology","Software","Application/system SW","Domestic","XLK,VGT,IGV","0-1%",5,"Intraday","Secular growth + rate sensitivity")
R(T,"Sector","Information Technology","Semiconductors","Chips/equipment","Domestic","SOXX,SMH","0-1%",5,"Intraday","Cyclical + AI capex bet")
R(T,"Sector","Information Technology","Hardware/IT services","Devices/services","Domestic","-","1%",4,"Intraday","Enterprise IT spend")
R(T,"Sector","Communication Services","Interactive media","Internet/social","Domestic","XLC,FCOM","0-1%",5,"Intraday","Ad cycle + platform moats")
R(T,"Sector","Communication Services","Telecom","Carriers","Domestic","VOX","4-6%",3,"Intraday","Income; capex heavy")
R(T,"Sector","Communication Services","Media/entertainment","Streaming/content","Domestic","PBS*","1%",5,"Intraday","Content/ad spend")
R(T,"Sector","Consumer Discretionary","Retail","E-comm/specialty","Domestic","XLY,VCR,XRT","0-1%",5,"Intraday","Consumer spending cycle")
R(T,"Sector","Consumer Discretionary","Autos","OEM/EV","Domestic","CARZ,DRIV","1%",5,"Intraday","Big-ticket cyclical")
R(T,"Sector","Consumer Discretionary","Homebuilders","Housing","Domestic","XHB,ITB","1%",5,"Intraday","Rate-sensitive housing")
R(T,"Sector","Consumer Discretionary","Leisure/travel","Hotels/airlines/cruise","Domestic","JETS,AWAY","0-1%",5,"Intraday","Discretionary travel demand")
R(T,"Sector","Consumer Staples","Food/beverage","Packaged food","Domestic","XLP,VDC","2-3%",3,"Intraday","Defensive; pricing power")
R(T,"Sector","Consumer Staples","Household/personal","HPC","Domestic","-","2-3%",3,"Intraday","Defensive staples")
R(T,"Sector","Health Care","Pharma","Big pharma","Domestic","XLV,VHT,PJP","1-2%",3,"Intraday","Patent cliffs + pipelines")
R(T,"Sector","Health Care","Biotech","Clinical/commercial","Domestic","XBI,IBB","0%",5,"Intraday","Binary trial risk")
R(T,"Sector","Health Care","Med devices/tools","Equipment","Domestic","IHI,XHE","0-1%",4,"Intraday","Procedure volumes")
R(T,"Sector","Health Care","Providers/managed care","Insurers/hospitals","Domestic","IHF","1%",4,"Intraday","Policy/reimbursement risk")
R(T,"Sector","Financials","Banks","Money-center/regional","Domestic","XLF,KBE,KRE","2-3%",4,"Intraday","Credit + rate cycle")
R(T,"Sector","Financials","Insurance","P&C/life","Domestic","KIE,IAK","2%",3,"Intraday","Underwriting + float")
R(T,"Sector","Financials","Capital markets","Brokers/exchanges/AM","Domestic","IAI","1-2%",4,"Intraday","Market activity beta")
R(T,"Sector","Financials","Fintech/payments","Networks/processors","Domestic","FINX,IPAY","0-1%",5,"Intraday","Payments growth")
R(T,"Sector","Industrials","Capital goods","Machinery/aerospace","Domestic","XLI,VIS","1-2%",4,"Intraday","Industrial cycle/capex")
R(T,"Sector","Industrials","Transportation","Rail/truck/air freight","Domestic","IYT,XTN","1-2%",4,"Intraday","Economic activity proxy")
R(T,"Sector","Industrials","Defense/aerospace","Primes","Domestic","ITA,PPA,XAR","1%",3,"Intraday","Defense budgets")
R(T,"Sector","Materials","Chemicals","Commodity/specialty","Domestic","XLB,VAW","1-2%",4,"Intraday","Input price cycle")
R(T,"Sector","Materials","Metals & mining","Diversified miners","Domestic","XME,PICK","2-3%",5,"Intraday","Commodity demand")
R(T,"Sector","Materials","Gold/silver miners","Precious miners","Domestic","GDX,GDXJ,SIL","0-1%",6,"Intraday","Leveraged metal price bet")
R(T,"Sector","Energy","Integrated/E&P","Oil & gas","Domestic","XLE,VDE,XOP","3-4%",5,"Intraday","Crude/gas price cycle")
R(T,"Sector","Energy","Midstream","Pipelines","Domestic","AMLP,MLPX","6-8%",4,"Intraday","Toll-road cash flows")
R(T,"Sector","Energy","Oil services","Equipment/services","Domestic","OIH","1-2%",6,"Intraday","Drilling capex beta")
R(T,"Sector","Energy","Clean energy","Solar/wind/hydrogen","Domestic","ICLN,TAN,FAN","0-1%",6,"Intraday","Policy + rate sensitive")
R(T,"Sector","Energy","Uranium/nuclear","Miners/fuel","Domestic","URA,URNM,NLR","0-1%",6,"Intraday","Nuclear demand thesis")
R(T,"Sector","Utilities","Regulated utilities","Electric/gas/water","Domestic","XLU,VPU","3%",3,"Intraday","Rate-sensitive income")
R(T,"Sector","Utilities","Water","Water utilities/infra","Domestic","PHO,FIW","1-2%",3,"Intraday","Scarcity/infra thesis")

# =====================================================================
# TIER 9 — INTERNATIONAL EQUITIES
# =====================================================================
T = "Intl Equities"
R(T,"Developed ex-US","Broad DM","EAFE","Developed intl","Developed ex-US","VEA,IEFA,EFA","2-3%",4,"Intraday","Foreign DM + FX")
R(T,"Developed ex-US","DM FX-hedged","Hedged EAFE","Currency-hedged","Developed ex-US","HEFA,HEDJ","2-3%",4,"Intraday","Strips FX")
R(T,"Developed ex-US","DM small cap","Intl small","Small DM","Developed ex-US","SCZ,GWX","2%",5,"Intraday","Foreign small-cap")
R(T,"Developed ex-US","Europe","Eurozone/UK","Regional Europe","Developed ex-US","VGK,FEZ,EWU","3%",4,"Intraday","Europe macro")
R(T,"Developed ex-US","Japan","Japan equity","Single-country","Developed ex-US","EWJ,DXJ,BBJP","2%",4,"Intraday","Japan reflation/governance")
R(T,"Developed ex-US","Canada/Australia","Resource DM","Single-country","Developed ex-US","EWC,EWA","3-4%",4,"Intraday","Commodity-linked DM")
R(T,"Emerging markets","Broad EM","EM core","EM equity","EM","VWO,IEMG,EEM","2-3%",5,"Intraday","EM growth + FX + political")
R(T,"Emerging markets","EM ex-China","De-China'd EM","EM ex-China","EM","EMXC","2-3%",5,"Intraday","EM minus China weight")
R(T,"Emerging markets","China","China equity","A/H/ADR","EM","MCHI,FXI,KWEB,ASHR","1-2%",5,"Intraday","China policy/regulatory risk")
R(T,"Emerging markets","India","India equity","Single-country","EM","INDA,EPI,SMIN","1%",5,"Intraday","India growth story")
R(T,"Emerging markets","Latin America","LatAm","Brazil/Mexico","EM","ILF,EWZ,EWW","3-5%",5,"Intraday","Commodity + politics")
R(T,"Emerging markets","EM Asia ex-China","Taiwan/Korea/SEA","Regional","EM","EWT,EWY,ASEA","2%",5,"Intraday","Tech supply chain")
R(T,"Frontier","Frontier markets","Frontier equity","Frontier","Frontier","FM,FRN","2-4%",6,"Daily","Illiquid, high political risk")
R(T,"Global","All-world","ACWI","Global equity","Global","ACWI,VT","2%",4,"Intraday","One-ticket global beta")
R(T,"Global","Intl dividend","Foreign dividend","Global div","Global","IDV,VIGI,VYMI","4-6%",3,"Intraday","Foreign income")

# =====================================================================
# TIER 10 — THEMATIC EQUITIES
# =====================================================================
T = "Thematic Equities"
R(T,"Technology themes","Artificial intelligence","AI/robotics","AI thematic","Global","BOTZ,ROBO,IRBO,AIQ","0%",6,"Intraday","AI adoption bet")
R(T,"Technology themes","Cybersecurity","Cyber","Security thematic","Global","CIBR,HACK,BUG","0%",6,"Intraday","Security spend growth")
R(T,"Technology themes","Cloud/SaaS","Cloud","Cloud thematic","Global","SKYY,WCLD","0%",6,"Intraday","Cloud migration")
R(T,"Technology themes","Fintech/blockchain","Fintech","Disruptive finance","Global","ARKF,BLOK","0%",6,"Intraday","Digital finance")
R(T,"Disruptive","Innovation","High-growth disruptors","Active thematic","Global","ARKK,ARKW","0%",6,"Intraday","Long-duration moonshots")
R(T,"Disruptive","Genomics","Gene editing","Genomics thematic","Global","ARKG,IDNA","0%",6,"Intraday","Biotech innovation")
R(T,"Disruptive","Space","Space economy","Space thematic","Global","UFO,ROKT","0%",6,"Intraday","Aerospace/space buildout")
R(T,"Demographics","Aging/longevity","Healthcare aging","Thematic","Global","-","1%",4,"Intraday","Demographic tailwind")
R(T,"ESG/values","ESG","Sustainability","ESG-screened","Global","ESGU,SUSA","1-2%",4,"Intraday","Values-screened beta")
R(T,"ESG/values","Clean transition","Battery/EV/clean","Energy transition","Global","LIT,BATT,DRIV","0-1%",6,"Intraday","Electrification bet")

# =====================================================================
# TIER 11 — REAL ESTATE
# =====================================================================
T = "Real Estate"
R(T,"Listed REITs","Broad REIT","Diversified","US REIT index","Domestic","VNQ,SCHH,IYR","3-4%",4,"Intraday","Property cycle + rates")
R(T,"Listed REITs","Residential","Apartments/SFR","Residential REIT","Domestic","REZ","3-4%",4,"Intraday","Rent growth")
R(T,"Listed REITs","Industrial/logistics","Warehouse","Industrial REIT","Domestic","INDS","3%",4,"Intraday","E-comm logistics")
R(T,"Listed REITs","Data centers","Digital infra","DC REIT","Domestic","DTCR,SRVR","2-3%",4,"Intraday","Cloud/AI compute demand")
R(T,"Listed REITs","Cell towers","Communications","Tower REIT","Domestic","-","3%",4,"Intraday","Mobile data growth")
R(T,"Listed REITs","Retail","Malls/strip","Retail REIT","Domestic","RTL*","4-6%",4,"Intraday","Brick-and-mortar risk")
R(T,"Listed REITs","Office","Office REIT","Office","Domestic","-","5-8%",5,"Intraday","WFH/vacancy risk")
R(T,"Listed REITs","Healthcare","Senior/medical","Healthcare REIT","Domestic","-","4-5%",4,"Intraday","Demographics + occupancy")
R(T,"Listed REITs","Self-storage","Storage","Storage REIT","Domestic","-","3-4%",4,"Intraday","Consumer storage demand")
R(T,"Listed REITs","Net lease","Triple-net","Net-lease REIT","Domestic","NETL","4-5%",3,"Intraday","Bond-like lease income")
R(T,"Mortgage REITs","Agency mREIT","Agency MBS levered","Agency mREIT","Domestic","REM,MORT","9-13%",6,"Intraday","Levered MBS carry; rate risk")
R(T,"Mortgage REITs","Commercial mREIT","CRE debt","Commercial mREIT","Domestic","-","8-11%",6,"Intraday","CRE credit + leverage")
R(T,"International RE","Global REIT","Ex-US property","Global REIT","Global","VNQI,RWX","3-4%",4,"Intraday","Foreign property + FX")
R(T,"Private RE","Non-traded REIT","Core/core-plus","Private real estate","Domestic","-","4-6%",5,"Periodic","Appraisal NAV; gates")
R(T,"Direct property","Physical RE","Residential/commercial","Direct ownership","Domestic","-","4-8%",5,"Illiquid","Leverage + concentration")
R(T,"Land","Raw/farmland","Agricultural land","Farmland","Domestic","FPI,LAND","2-4%",4,"Daily","Crop income + appreciation")

# =====================================================================
# TIER 12 — INFRASTRUCTURE & MLPs
# =====================================================================
T = "Infrastructure"
R(T,"Listed infra","Core infrastructure","Global infra","Listed infra","Global","IGF,NFRA","2-4%",3,"Intraday","Regulated, inflation-linked")
R(T,"MLPs","Midstream MLP","Pipelines/storage","Energy MLP","Domestic","AMLP,MLPA","6-8%",4,"Intraday","K-1; toll-road cash flow")
R(T,"Digital infra","Towers/data/fiber","Communications infra","Digital infra","Global","-","2-3%",4,"Intraday","Data growth")
R(T,"Renewable infra","Yieldcos","Wind/solar assets","Renewable infra","Global","-","4-6%",4,"Daily","Contracted power; rate sensitive")
R(T,"Private infra","Unlisted infra","Brownfield/greenfield","Private infra","Global","-","5-8%",5,"Illiquid","Long-dated, illiquid")

# =====================================================================
# TIER 13 — COMMODITIES
# =====================================================================
T = "Commodities"
R(T,"Broad","Diversified basket","Multi-commodity","Broad commodity","Global","DBC,PDBC,GSG,BCI","0%",4,"Intraday","Inflation/real-asset hedge")
R(T,"Precious metals","Gold","Bullion","Physical gold","Global","GLD,IAU,GLDM","0%",3,"Intraday","Monetary/safe-haven hedge")
R(T,"Precious metals","Silver","Bullion","Physical silver","Global","SLV,SIVR","0%",4,"Intraday","Monetary + industrial")
R(T,"Precious metals","Platinum/palladium","PGM","Physical PGM","Global","PPLT,PALL","0%",5,"Intraday","Auto/industrial demand")
R(T,"Energy","Crude oil","WTI/Brent","Oil futures","Global","USO,BNO","0%",5,"Intraday","Crude price; roll yield")
R(T,"Energy","Natural gas","Henry Hub","Gas futures","Global","UNG,BOIL","0%",6,"Intraday","High vol; contango decay")
R(T,"Energy","Carbon","Carbon allowances","Emissions","Global","KRBN","0%",5,"Intraday","Climate-policy bet")
R(T,"Base metals","Copper","Copper","Copper futures/eq","Global","CPER,COPX","0%",5,"Intraday","Electrification/China demand")
R(T,"Base metals","Aluminum/nickel/zinc","Industrial metals","Base metals","Global","DBB","0%",5,"Intraday","Industrial cycle")
R(T,"Base metals","Battery metals","Lithium/cobalt","Battery materials","Global","LIT,REMX","0%",6,"Intraday","EV supply chain")
R(T,"Agriculture","Grains","Corn/wheat/soy","Grains","Global","DBA,CORN,WEAT,SOYB","0%",5,"Intraday","Weather/supply shocks")
R(T,"Agriculture","Softs","Coffee/sugar/cocoa","Softs","Global","NIB,CANE,JO","0%",6,"Intraday","Weather/geopolitics")
R(T,"Agriculture","Livestock","Cattle/hogs","Livestock","Global","COW*","0%",5,"Intraday","Herd cycles")
R(T,"Physical","Allocated metals","Vaulted bullion","Physical holdings","Global","-","0%",3,"Periodic","Storage/insurance cost")

# =====================================================================
# TIER 14 — ALTERNATIVES / HEDGE STRATEGIES
# =====================================================================
T = "Alternatives"
R(T,"Liquid alts","Managed futures","Trend following","CTA","Global","DBMF,KMLM,CTA","0%",4,"Daily","Crisis-alpha; trend diversifier")
R(T,"Liquid alts","Global macro","Discretionary/systematic","Macro","Global","-","0%",4,"Daily","Top-down cross-asset bets")
R(T,"Liquid alts","Market neutral","Equity long/short","Market neutral","Global","BTAL,MNA*","2-4%",3,"Daily","Low beta; spread capture")
R(T,"Liquid alts","Merger arbitrage","Event-driven","Merger arb","Global","MNA,MERFX","3-5%",3,"Daily","Deal-spread capture")
R(T,"Liquid alts","Long/short equity","Hedged equity","L/S equity","Global","-","0-2%",4,"Daily","Net long with hedges")
R(T,"Liquid alts","Multi-strategy","Diversified alts","Multi-strat","Global","QAI","2-4%",3,"Daily","Blended alt sleeves")
R(T,"Liquid alts","Risk parity","Levered diversified","Risk parity","Global","RPAR,UPAR","2-3%",4,"Daily","Vol-balanced multi-asset")
R(T,"Liquid alts","Return stacking","Capital efficient","Stacked beta+alt","Global","RSST,RSBT","1-3%",4,"Daily","Overlay alts on core beta")
R(T,"Hedge funds","Private hedge funds","LP structures","HF strategies","Global","-","varies",5,"Periodic","Lockups; 2/20; manager risk")
R(T,"Insurance-linked","Catastrophe bonds","Reinsurance risk","Cat bonds","Global","-","8-12%",5,"Periodic","Uncorrelated; tail event loss")
R(T,"Royalties","Music/pharma/mineral","Income royalties","Royalty streams","Global","-","6-10%",4,"Periodic","Cash-flow streams")
R(T,"Litigation finance","Legal funding","Case portfolios","Litigation finance","Global","-","high",6,"Illiquid","Binary case outcomes")

# =====================================================================
# TIER 15 — PRIVATE EQUITY / VENTURE
# =====================================================================
T = "Private Equity"
R(T,"Listed PE","PE proxies","Listed sponsors","PE-linked equity","Domestic","PSP,KKR,BX,APO","2-4%",5,"Intraday","Public proxy for PE")
R(T,"Buyout","LBO funds","Large/mid buyout","Buyout","Global","-","-",6,"Illiquid","Leverage + operational alpha")
R(T,"Growth equity","Growth funds","Pre-IPO growth","Growth equity","Global","-","-",6,"Illiquid","Late-stage scaling")
R(T,"Venture capital","VC funds","Seed/early/late","Venture","Global","-","-",6,"Illiquid","Power-law return; J-curve")
R(T,"Secondaries","PE secondaries","LP stakes","Secondaries","Global","-","-",5,"Illiquid","Discounted NAV entry")
R(T,"Co-investment","Direct co-invest","Deal-by-deal","Co-investment","Global","-","-",6,"Illiquid","Concentrated single-deal")
R(T,"Interval/evergreen","Evergreen PE","Retail-access PE","Evergreen","Global","-","-",5,"Periodic","Quarterly gates; NAV marks")

# =====================================================================
# TIER 16 — VOLATILITY & OPTIONS STRATEGIES
# =====================================================================
T = "Volatility & Options"
R(T,"Long vol","VIX futures","Short-term VIX","Long volatility","Domestic","VIXY,VXX,UVXY","0%",6,"Intraday","Hedge; severe roll decay")
R(T,"Tail risk","Tail hedge","Convexity","Tail risk","Domestic","TAIL","0%",5,"Intraday","Crash insurance; bleeds")
R(T,"Income","Covered call","Buy-write","Option income","Domestic","JEPI,JEPQ,QYLD,XYLD","7-12%",4,"Intraday","Caps upside for income")
R(T,"Income","Put-write","Cash-secured puts","Put income","Domestic","PUTW","5-8%",4,"Intraday","Sells downside insurance")
R(T,"Defined outcome","Buffered/defined","Structured ETF","Buffer/floor","Domestic","BUFR,PBP","varies",3,"Intraday","Caps gains for downside buffer")
R(T,"Short vol","Inverse VIX","Short volatility","Short vol","Domestic","SVIX,SVXY","0%",6,"Intraday","Sells vol; blow-up risk")

# =====================================================================
# TIER 17 — DIGITAL ASSETS
# =====================================================================
T = "Digital Assets"
R(T,"Crypto","Bitcoin","Spot BTC","Store of value","Global","IBIT,FBTC,GBTC","0%",6,"Intraday","Digital gold thesis; high vol")
R(T,"Crypto","Ethereum","Spot ETH","Smart-contract L1","Global","ETHA,ETHE","0-4%*","6","Intraday","Platform bet; *staking yield")
R(T,"Crypto","Large-cap alts","L1/L2","Altcoins","Global","-","0%",6,"Intraday","High beta to BTC")
R(T,"Crypto","Stablecoin yield","Lending/staking","DeFi yield","Global","-","4-10%",5,"Intraday","Smart-contract/counterparty risk")
R(T,"Crypto","Crypto equity","Miners/exchanges","Crypto-linked stocks","Global","COIN,MSTR,WGMI","0%",6,"Intraday","Levered crypto proxy")
R(T,"Tokenized","RWA tokens","Tokenized T-bills/credit","Tokenized RWA","Global","-","4-8%",4,"Intraday","On-chain real assets")

# =====================================================================
# TIER 18 — CURRENCIES / FX
# =====================================================================
T = "Currencies / FX"
R(T,"FX","USD","Dollar index","Long/short USD","Global","UUP,UDN","0-4%",3,"Intraday","Dollar direction")
R(T,"FX","DM currencies","EUR/JPY/GBP/CHF","DM FX","Developed ex-US","FXE,FXY,FXB,FXF","0-2%",3,"Intraday","Rate differentials")
R(T,"FX","EM currencies","EM FX basket","EM FX","EM","CEW","2-5%",4,"Intraday","Carry + EM risk")
R(T,"FX","Carry trade","High-yield FX","Carry","Global","DBV*","3-6%",4,"Daily","Carry; crash risk")
R(T,"FX","Gold as currency","Monetary metal","Anti-fiat","Global","GLD,IAU","0%",3,"Intraday","Currency-debasement hedge")

# =====================================================================
# TIER 19 — COLLECTIBLES / ESOTERIC REAL ASSETS
# =====================================================================
T = "Collectibles / Esoteric"
R(T,"Collectibles","Fine art","Blue-chip art","Art","Global","Masterworks*","0%",6,"Illiquid","Taste-driven; illiquid")
R(T,"Collectibles","Fine wine/spirits","Vintage wine","Wine","Global","Vinovest*","0%",5,"Illiquid","Storage; provenance")
R(T,"Collectibles","Watches/jewelry","Luxury timepieces","Watches","Global","-","0%",6,"Illiquid","Brand/condition dependent")
R(T,"Collectibles","Cars","Classic/collector autos","Cars","Global","-","0%",6,"Illiquid","Maintenance; taste")
R(T,"Collectibles","Trading cards/memorabilia","Cards/sports","Collectibles","Global","-","0%",6,"Illiquid","Fad risk")
R(T,"IP/royalties","Music catalogs","Royalty income","Music IP","Global","SONG*","5-8%",4,"Periodic","Streaming income streams")
R(T,"Esoteric","Whiskey casks","Maturing spirits","Casks","Global","-","0%",5,"Illiquid","Maturation/storage")
R(T,"Esoteric","Diamonds/gems","Hard assets","Gems","Global","-","0%",6,"Illiquid","Opaque pricing")

# ---------------------------------------------------------------------
# Write CSV master
# ---------------------------------------------------------------------
csv_path = os.path.join(OUT_DIR, "asset_universe.csv")
with open(csv_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(COLUMNS)
    for row in ROWS:
        w.writerow(row)

# ---------------------------------------------------------------------
# Write Excel workbook (Master + per-Tier sheets + Legend)
# ---------------------------------------------------------------------
wb = Workbook()

header_fill = PatternFill("solid", fgColor="1F3864")
header_font = Font(color="FFFFFF", bold=True, size=11)
tier_fill = PatternFill("solid", fgColor="D9E1F2")
thin = Side(style="thin", color="BFBFBF")
border = Border(left=thin, right=thin, top=thin, bottom=thin)
center = Alignment(horizontal="center", vertical="center")
wrap = Alignment(vertical="top", wrap_text=True)

risk_colors = {
    1: "C6EFCE", 2: "D9EAD3", 3: "FFF2CC",
    4: "FCE5CD", 5: "F4CCCC", 6: "EA9999",
}

WIDTHS = [20, 22, 24, 26, 26, 16, 30, 12, 9, 12, 46]

def style_sheet(ws, rows, with_tracking=False):
    headers = COLUMNS + (["Hold?", "Position $", "Target %", "Notes/Action"] if with_tracking else [])
    ws.append(headers)
    for c, _ in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c)
        cell.fill = header_fill; cell.font = header_font
        cell.alignment = center; cell.border = border
    for row in rows:
        ws.append(list(row) + (["", "", "", ""] if with_tracking else []))
    # widths
    widths = WIDTHS + ([8, 12, 9, 28] if with_tracking else [])
    for i, wdt in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = wdt
    # style body
    for r in range(2, ws.max_row + 1):
        for c in range(1, len(headers) + 1):
            cell = ws.cell(row=r, column=c)
            cell.border = border
            cell.alignment = center if c in (8, 9, 10) else wrap
        # color risk tier
        rt = ws.cell(row=r, column=9).value
        try:
            rt = int(rt)
            ws.cell(row=r, column=9).fill = PatternFill("solid", fgColor=risk_colors.get(rt, "FFFFFF"))
        except (TypeError, ValueError):
            pass
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

# Master sheet with tracking columns
ws_master = wb.active
ws_master.title = "MASTER (all assets)"
style_sheet(ws_master, ROWS, with_tracking=True)

# Per-tier sheets
tiers = []
for row in ROWS:
    if row[0] not in tiers:
        tiers.append(row[0])
for tier in tiers:
    safe = tier.replace("/", "-")[:31]
    ws = wb.create_sheet(title=safe)
    style_sheet(ws, [r for r in ROWS if r[0] == tier], with_tracking=False)

# Legend / README sheet
ws_leg = wb.create_sheet(title="LEGEND", index=1)
legend = [
    ["Asset Universe — Systematic Tracking Taxonomy", ""],
    ["", ""],
    ["Hierarchy", "Tier > Asset Class > Sub-Class > Sector/Strategy > Sub-Sector"],
    ["Geography", "Domestic (US) / Developed ex-US / EM / Frontier / Global"],
    ["", ""],
    ["Risk Tier scale", ""],
    ["1", "Capital preservation (cash, T-bills, AAA CLO, agency MBS)"],
    ["2", "Low risk (short IG, TIPS, mezz CLO debt)"],
    ["3", "Income + some protection (preferreds, utilities, gold)"],
    ["4", "Moderate (broad equity, HY, REITs, commodities, CLO BB)"],
    ["5", "Elevated (small cap, EM, BDCs, sector bets, loans)"],
    ["6", "Max beta / first loss (CLO equity, crypto, VIX, biotech, PE/VC)"],
    ["", ""],
    ["Liquidity", "Intraday / Daily / Periodic (gated) / Illiquid (lockup)"],
    ["Typ. Yield", "Rough illustrative income range; TE = tax-exempt; *=conditional"],
    ["", ""],
    ["DISCLAIMER", "Illustrative taxonomy for tracking/organization only."],
    ["", "Tickers are examples to anchor categories, NOT recommendations."],
    ["", "Yields/risk tiers are approximate as of generation, not advice."],
    ["", ""],
    ["The original CLO ladder maps to:", ""],
    ["1 Safest — AAA CLO debt", "Securitized Credit > CLO > CLO debt > AAA (JAAA)"],
    ["2 Sweet spot — Mezz CLO debt", "Securitized Credit > CLO > CLO debt > BBB (JBBB,CLOZ)"],
    ["3 Income+protection — CLO-CEF pfd", "Preferred & Hybrid > CLO-CEF preferreds (OXLCN/I/Z,ECC-D)"],
    ["4 Raw asset — Senior loans", "Loans > Senior loans (BKLN,SRLN)"],
    ["5 Direct private credit — BDC", "Private Credit > BDC (ARCC,BIZD)"],
    ["6 Max beta — CLO equity", "Securitized Credit > CLO > CLO equity (ECC,OXLC)"],
]
for r in legend:
    ws_leg.append(r)
ws_leg.column_dimensions["A"].width = 38
ws_leg.column_dimensions["B"].width = 70
ws_leg["A1"].font = Font(bold=True, size=14, color="1F3864")
for r in range(1, ws_leg.max_row + 1):
    a = ws_leg.cell(row=r, column=1)
    if a.value in ("Risk Tier scale", "DISCLAIMER", "The original CLO ladder maps to:", "Hierarchy"):
        a.font = Font(bold=True)
    ws_leg.cell(row=r, column=2).alignment = wrap

xlsx_path = os.path.join(OUT_DIR, "asset_universe.xlsx")
wb.save(xlsx_path)

print(f"Rows: {len(ROWS)}")
print(f"Tiers: {len(tiers)}")
print(f"CSV : {csv_path}")
print(f"XLSX: {xlsx_path}")
