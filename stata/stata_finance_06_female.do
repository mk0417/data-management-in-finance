clear

cd "your_working_directory"

import delimited execucomp.txt, clear

/* Rename year */
rename year fyear

/* Check duplicates */
duplicates drop gvkey fyear execid, force

/* Count totoal number of executives */
bysort gvkey fyear: egen n_total = count(execid)

/* Count number of female executives */
gen gender_id = 1 if gender=="FEMALE"
replace gender_id = 0 if gender=="MALE"
bysort gvkey fyear: egen n_female = sum(gender_id)

/* Count number of female CEO */
gen gender_ceo = 1 if gender=="FEMALE" & ceoann=="CEO"
replace gender_ceo = 0 if gender_ceo == .
bysort gvkey fyear: egen n_female_ceo = sum(gender_ceo)

duplicates drop gvkey fyear, force

gen pct_female = n_female / n_total
gen pct_female_ceo = n_female_ceo / n_total

collapse (mean) pct_female=pct_female pct_female_ceo=pct_female_ceo, by(fyear)

line pct_female pct_female_ceo fyear
graph export pct_female_do.eps, replace

