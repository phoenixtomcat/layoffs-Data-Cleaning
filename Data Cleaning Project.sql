-- Data Cleaning

select * 
from layoffs;

-- 1. Remove Duplicates
# Disable safe update mode in Edit, Preferences, SQL editor, and uncheck at the bottom
# Then reconnect SQL to server, Query -> Reconnect to Server
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove unnecessary rows or columns 

create table layoffs_staging 
like layoffs; # create a table like the layoffs so we can keep the original untouched

select * 
from layoffs_staging; # columns are same as layoffs, ready for data 

insert layoffs_staging
select *
from layoffs; # run this to insert, then run above to show 

-- 1. Remove Duplicates 

select *, 
row_number() over ( 
partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num # partition over all these columns
from layoffs_staging; # date is done with the back-ticks under Esc key, because date is a keyword in mySQL
# row_num is unique 

with duplicate_cte as
(
select *, 
row_number() over ( 
partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1; # shows you the duplicates 

select *
from layoffs_staging
where company = 'Casper';


with duplicate_cte as
(
select *, 
row_number() over ( 
partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging
)
delete # Error: cannot update a CTE, and delete is an update command
from duplicate_cte
where row_num > 1;

# create another staging table and delete rows from there 
# On the left, right click on the table layoffs_staging, then Copy to Clipboard -> Create Statement. Paste down.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2; # Empty table, with said columns

# Copy the contents of the CTE and paste 

Insert into layoffs_staging2
select *, 
row_number() over ( 
partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging; # Run this. Then run above. 

select *
from layoffs_staging2
where row_num > 1; # show what you want to delete 

delete
from layoffs_staging2
where row_num > 1; # delete the duplicates 

select *
from layoffs_staging2; # see what remains

-- 2. Standardizing Data 

select company, (trim(company)) # shows both the normal company and the trimmed company name which removes fluff around it
from layoffs_staging2;

update layoffs_staging2
set company = trim(company); # if trim(company) looked good, set the company to it. Re-run above to show 

select distinct industry # show unique values only
from layoffs_staging2
order by 1; # order by the first column - which will show NULL and blanks. You also have duplicates like Crypto, CryptoCurrency and Crypto Currency - should all be one. Unless told not to! 

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'; # Update the table, by setting industry to Crypto, if industry has anything that resembles Crypto%

select distinct industry 
from layoffs_staging2
order by 1; # show all the unique industry and see if Crypto is just one

select distinct location
from layoffs_staging2
order by 1; # check entries for location 

select distinct country
from layoffs_staging2
order by 1; # check entries for Country. United States is repeated twice, one with an extra dot. 

select *
from layoffs_staging2
where country like 'United States%' # show me everything that resembles United States%, order by column 1 
order by 1;

select distinct country, trim(country) # it won't fix it immediately, but you can do Trailing
from layoffs_staging2
where country like 'United States%' 
order by 1;

select distinct country, trim(trailing '.' from country) # it won't fix it immediately, but you can do Trailing which comes at the end
from layoffs_staging2
where country like 'United States%' 
order by 1;

update layoffs_staging2 # update the main table 
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`, # Shows date, which when importing, was of type text, NOT date !!
from layoffs_staging2;

select `date`, # changing the type 
str_to_date(`date`, '%m/%d/%Y') # str_to_date is the function to change. %m is for month type, %d for day, %Y capital Y for 4 number long year 
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y'); # change the table with the changes 


select `date`# Shows date, with the changed type. There are NULLS here 
from layoffs_staging2;

# going to the date tab on the left side, it still shows date as text. We will change it now 

alter table layoffs_staging2 # Never run this on production table. Always run it on your staging tables
modify column `date` date; # run this, refresh on the left, and check the date format. It changed ! 

select *
from layoffs_staging2
where total_laid_off is NULL; # lots of nulls in the total laid off col. But having it in percentage laid off too...
# we might have to take a look at that later

select *
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off IS NULL; # perhaps these are useless 

select distinct industry
from layoffs_staging2; # we also had blanks and NULL here 

select *
from layoffs_staging2
where industry is NULL
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb'; # another instance of Airbnb has the industry listed as "Travel". so we can use that

select t1.industry, t2.industry
from layoffs_staging2 as t1 # this will be t1
join layoffs_staging2 as t2 # join itself, referred to as t2, as the copy to table
	on t1.company = t2.company # the company 
    and t1.location = t2.location  # the location must be the same, in case a company with the same name exists in another country
where (t1.industry is null or t1.industry = '')
and t2.industry is not null; 

update layoffs_staging2 as t1
join layoffs_staging2 as t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null; # Rows Matched: 9, but 0 Changed ???

update layoffs_staging2
set industry = NULL 
where industry = ''; # lets fix that by forcing all those that are blank to become NULL, and THEN run the above, then see how it is! 
# which worked ! 

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off IS NULL; # shows date of lay offs, but both columns are NULL, which is odd. So I think it's useless.

delete
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off IS NULL; # remove them


alter table layoffs_staging2
drop column row_num; # drop row num because we don't need it anymore. then run below to show the finalized table :D

select *
from layoffs_staging2;
