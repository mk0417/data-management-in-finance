clear

cd "your_working_directory"

/* Read data from Bloomberg */
import excel bloomberg_data, sheet("Sheet3") firstrow clear

local c = 1
foreach i of varlist GB00B1XZS820Equity-GB00B1KJJ408Equity {
rename `i' firm`c'
local c = `c'+1
}

/* Drop firms with missing data in all years */
ds, has(type string)
return list
drop `r(varlist)'

/* Transpose data */
reshape long firm, i(date) j(firmid)
rename firm assets
order firmid date
sort firmid date

