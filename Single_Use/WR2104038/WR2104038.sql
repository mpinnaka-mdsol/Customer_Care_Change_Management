--********************************************************************************************************
-- * Author: Murali Pinnaka
-- * Create Date: Feb 08 2016
-- * Work Request: 2104038
-- * Rave Version Developed For:2014.2.3
-- * URL: 	bostonscientific-trials.mdsol.com
-- * Module: EDC
--********************************************************************************************************
--********************************************************************************************************
-- * Description: Fill in missing datapoints for subjects.
-- * Affect project 'ZERO_AF' in Prod environments where subjects are EN 1630ZA004 and EN 1798ZA029.


-------------------------------------------------------------
-- ** Subject: EN 1630ZA004 **--
-------------------------------------------------------------

exec dbo.spMigrationFillInMissingDataPoints 93030,'eng'

-------------------------------------------------------------
-- ** Subject: EN 1798ZA029 **--
-------------------------------------------------------------
exec dbo.spMigrationFillInMissingDataPoints 92120,'eng'



