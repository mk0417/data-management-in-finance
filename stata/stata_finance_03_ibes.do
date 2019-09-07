clear

cd "your_working_directory"

// Read data drop duplicates
import delimited ibes_1976_1990_summ_both.txt, clear
duplicates drop ticker statpers, force

// Check 1-year EPS: use either codebook or count
codebook measure fpi

count if missing(measure)
count if missing(fpi)

// US sample
tab usfirm

bysort usfirm: count

keep if usfirm == 1

// Sample selection: keep firms with at least 60 month of numest
bysort ticker: egen num_month = count(numest)
keep if num_month >= 60

// firm-month observations
count

// Generate firmid to check number of unique firm
egen firmid = group(ticker)
order firmid
tabstat firmid, s(max)

// Basic statistics
summ numest meanest stdev
summ numest meanest stdev, d
gen year = int(statpers/10000)
bysort year: summ numest meanest stdev

tabstat numest meanest stdev
help tabstat // check the list of available statistics
tabstat numest meanest stdev, s(n mean sd min p1 median p99 max)
tabstat numest meanest stdev, s(n mean sd min p1 median p99 max) col(stat)
tabstat numest meanest stdev, by(year) s(n mean sd min p1 median p99 max)

// Percentile
centile (numest), centile(10,20,30,40,50,60,70,80,90)

egen p10 = pctile(numest), p(10) by(year)
egen p20 = pctile(numest), p(20) by(year)
egen p30 = pctile(numest), p(30) by(year)
egen p40 = pctile(numest), p(40) by(year)
egen p50 = pctile(numest), p(50) by(year)
egen p60 = pctile(numest), p(60) by(year)
egen p70 = pctile(numest), p(70) by(year)
egen p80 = pctile(numest), p(80) by(year)
egen p90 = pctile(numest), p(90) by(year)

drop p10-p90
forvalues i = 10(10)90 {
    egen p`i' = pctile(numest), p(`i') by(year)
}

// Correlation
corr numest meanest stdev

