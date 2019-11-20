clear
set more off

cd "/users/ml/dropbox/teaching/data/"

use merged, clear

describe

rename ret ret_str
gen ret = real(ret_str)
drop ret_str

gen year = int(yyyymm/100)

duplicates drop cusip year, force

sort cusip year

gen roa = ib / at
gen roe = ib / seq
gen lnme = ln(me)
gen lnat = ln(at)

foreach i in me lnme at lnat roa roe year {
    by cusip: gen lag_`i' = `i'[_n-1]
}

gen year_diff = year - lag_year

summ year_diff

return list
list cusip year lag_year year_diff if year_diff==r(max)

foreach i in me lnme at lnat roa roe {
    replace lag_`i' = . if year_diff!=1
}

gen ag = at / lag_at - 1

local var_list me lnme at lnat roa roe ag
summ `var_list'

/* Winsorize */
foreach i in `var_list' {
    bysort year: egen p1_`i' = pctile(`i'), p(1)
    bysort year: egen p99_`i' = pctile(`i'), p(99)
    replace `i' = p1_`i' if `i'<p1_`i'
    replace `i' = p99_`i' if `i'>p99_`i'
}

drop p1_* p99_*

sort cusip year

/* Pooled OLS */
reg ret lag_me,

reg ret lag_lnme,
reg ret lag_lnme, robust

gen a = _b[_cons]
gen b = _b[lag_lnme]

gen y_calc = a + b*lag_lnme
gen e_calc = ret - y_calc

predict y_est
predict e_est, resid


/* Panel regression */
egen stock_id = group(cusip)
xtset stock_id year

xtreg ret lag_lnme, fe

xtreg ret lag_lnme, fe robust


/* Pooled vs panel */
import excel reg_demo_data, firstrow clear

reg ret size

xtset stock year

xtreg ret size, fe

