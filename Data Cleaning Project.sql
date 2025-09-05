-- Data Cleaning --

select *
from layoffs;

-- creating staging data --

create table layoffs_staging 
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- checking for duplicate data --

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Casper';

-- creating secondary table with row_num to delete duplicates --

CREATE TABLE `layoffs_staging2` ( 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- removing duplicates --

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num = 2;

delete 					
from layoffs_staging2
where row_num > 1;

-- Standardizing Data --

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company); -- getting rid of unecessary whitespaces --

select distinct industry
from layoffs_staging2
order by 1;

-- found same industries with slightly different names so we can combine --

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- checking for any locations that could be combined --

select distinct location
from layoffs_staging2
order by 1;

select distinct country, trim(TRAILING '.' FROM country) -- looking for extra period behind country --
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(TRAILING '.' FROM country)
where country like 'United States%';

-- changing the type of the column --

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

-- fixing nulls or blanks where we can --

select *
from layoffs_staging2
where total_laid_off is null 	
and percentage_laid_off is null;

select * 
from layoffs_staging2
where industry is NULL
or industry ='';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = '';

-- Filling in blank industry column where we can --

update layoffs_staging2 t1		
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- getting rid of unecessary rows/columns --

select *						
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


