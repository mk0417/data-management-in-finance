clear
set more off

cd ""

/* Import FF 3-factors */
import delimited "ff_factors.csv", clear
save _junk, replace

/* Import 25 (5x5) portfolios formed on size and book-to-market ratio */
import delimited "25portfolios.csv", clear
ren smalllobm me1bm1
ren smallhibm me1bm5
ren biglobm me5bm1
ren bighibm me5bm5

/* Merge with FF 3-factors */
merge 1:1 date using _junk
keep if _merge == 3
drop _merge
mvdecode _all, mv(-99.99 -999)
rm _junk.dta

/* Calculate excess return */
foreach i of varlist me1bm1-me5bm5 {
	replace `i' = `i' - rf
}

/* CAPM and 25 (5x5) portfolios formed on market value and
 book-to-makret ratio (before 1965) */
mat capm_25port_bef = J(25,8,0)
mat colnames capm_25port_bef = obs mean std alpha b_mkt t_alpha t_mkt r2_adj

local rnames
local k = 1
forvalues i = 1/5 {
	forvalues j = 1/5 {
		local rnames `rnames' me`i'bm`j'
		qui sum me`i'bm`j' if date < 196501
		mat capm_25port_bef[`k',1] = round(r(N),0.01)
		mat capm_25port_bef[`k',2] = round(r(mean),0.01)
		mat capm_25port_bef[`k',3] = round(r(sd),0.01)
		qui reg me`i'bm`j' mktrf if date < 196501
		mat capm_25port_bef[`k',4] = round(_b[_cons],0.001)
		mat capm_25port_bef[`k',5] = round(_b[mktrf],0.001)
		mat capm_25port_bef[`k',6] = round(_b[_cons]/_se[_cons],0.01)
		mat capm_25port_bef[`k',7] = round(_b[mktrf]/_se[mktrf],0.01)
		mat capm_25port_bef[`k',8] = round(e(r2_a),0.01)
		local ++k
	}
}

mat rownames capm_25port_bef = `rnames'


/* CAPM and 25 (5x5) portfolios formed on market value and
 book-to-makret ratio (after 1965) */
mat capm_25port_aft= J(25,8,0)
mat colnames capm_25port_aft = obs mean std alpha b_mkt t_alpha t_mkt r2_adj

local rnames
local k = 1
forvalues i = 1/5 {
	forvalues j = 1/5 {
		local rnames `rnames' me`i'bm`j'
		qui sum me`i'bm`j' if date >= 196501
		mat capm_25port_aft[`k',1] = round(r(N),0.01)
		mat capm_25port_aft[`k',2] = round(r(mean),0.01)
		mat capm_25port_aft[`k',3] = round(r(sd),0.01)
		qui reg me`i'bm`j' mktrf if date >= 196501
		mat capm_25port_aft[`k',4] = round(_b[_cons],0.001)
		mat capm_25port_aft[`k',5] = round(_b[mktrf],0.001)
		mat capm_25port_aft[`k',6] = round(_b[_cons]/_se[_cons],0.01)
		mat capm_25port_aft[`k',7] = round(_b[mktrf]/_se[mktrf],0.01)
		mat capm_25port_aft[`k',8] = round(e(r2_a),0.01)
		local ++k
	}
}

mat rownames capm_25port_aft = `rnames'

