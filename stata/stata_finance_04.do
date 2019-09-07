clear

cd "your_working_directory"

/* Import firm total asset */
import excel using datastream_data, sheet("asset_1") first clear
rename Code year

/* Rename */
ds year, not
return list
rename (`r(varlist)') asset#, addnumber

/* Reshape wide to long */
reshape long asset, i(year) j(firmid)
sort firmid year
save datastream_asset,replace

/* Import firm list and merge firm total asset */
import excel using datastream_data, sheet("firm_list") first clear

merge 1:m firmid using datastream_asset
drop _merge
save datastream_asset,replace

/* Import fiscal year end and reshape */
import excel using datastream_data, sheet("fyend_1") first clear
rename Code year
reshape long firm, i(year) j(firmid)
rename firm fyear

/* Merge firm total asset */
merge 1:1 firmid year using datastream_asset
drop _merge firmid
gen temp_month = substr(fyear,1,2)
gen fmonth = real(subinstr(temp_month,"/","",.))
drop temp_month
order isin year
sort isin year
save datastream_data, replace


