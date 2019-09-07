clear

cd "~/Dropbox/teaching/data"

/* Read data */
import delimited type1.txt, clear
keep if fdate==rdate

/* Count number of institutional investors by type */
collapse (count) n=mgrno, by(rdate typecode)

/* Reshape */
reshape wide n, i(rdate) j(typecode)
sort rdate

/* Label multiple variables: method 1 */
label variable n1 bank
label variable n2 insurance
label variable n3 mutual_fund
label variable n4 advisor
label variable n5 other

/* Label multiple variables: method 2 */
/* This is will be more efficient if you need to label many variables */
local oldvars "n1 n2 n3 n4 n5"
local newlabels "bank insurance mutual_fund advisor other"
local n: word count `oldvars'

forvalues i=1/`n'{
    local j: word `i' of `oldvars'
    local k: word `i' of `newlabels'
    label variable `j' `k'
}

egen total = rowtotal(n1 n2 n3 n4 n5)
gen date = date(string(rdate,"%8.0f"),"YMD")
format date %td

/* Time series plot */
tsset date, daily
graph twoway tsline n1 n2 n3 n4 n5 total, tlabel(31jan2005 31jan2010 31jan2015)
graph export n_inst.eps, replace


