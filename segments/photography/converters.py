def to_binary(x, true_value, false_value):
    if x == true_value:
        return 1.0
    if x == false_value:
        return 0.0
    return None


def get_age(age_range):
    if age_range == 'A: 18-24 years old':
        return 21.0
    if age_range == 'B: 25-34 years old':
        return 30.0
    if age_range == 'C: 35-44 years old':
        return 40.0
    if age_range == 'D: 45-54 years old':
        return 50.0
    if age_range == 'E: 55-64 years old':
        return 60.0
    if age_range == 'F: 65-74 years old':
        return 70.0
    if age_range == 'G: 75+ years old':
        return 80
    return None


def get_home_value(home_value_range):
    if home_value_range == 'A. Under $50k':
        return 35000.0
    if home_value_range == 'B. $50 - $100k':
        return 75000.0
    if home_value_range == 'C. $100 - $150k':
        return 125000.0
    if home_value_range == 'D. $150 - $200k':
        return 175000.0
    if home_value_range == 'E. $200 - $250k':
        return 225000.0
    if home_value_range == 'F. $250 - $300k':
        return 275000.0
    if home_value_range == 'G. $300 - $350k':
        return 325000.0
    if home_value_range == 'H. $350 - $400k':
        return 375000.0
    if home_value_range == 'I. $400 - $450k':
        return 425000.0
    if home_value_range == 'J. $450 - $500k':
        return 475000.0
    if home_value_range == 'K. $500 - $550k':
        return 525000.0
    if home_value_range == 'L. $550 - $600k':
        return 575000.0
    if home_value_range == 'M. $600 - $650k':
        return 625000.0
    if home_value_range == 'N. $650 - $700k':
        return 675000.0
    if home_value_range == 'O. $700 - $750K':
        return 725000.0
    if home_value_range == 'P. $750K +':
        return 800000.0
    return None


def get_income(income_range_vds):
    if income_range_vds == 'A: Under $15,000':
        return 10000.0
    if income_range_vds == 'B: $15,000 - $19,999':
        return 17250.0
    if income_range_vds == 'C: $20,000 - $29,999':
        return 25000.0
    if income_range_vds == 'D: $30,000 - $39,999':
        return 35000.0
    if income_range_vds == 'E: $40,000 - $49,999':
        return 45000.0
    if income_range_vds == 'F: $50,000 - $74,999':
        return 62500.0
    if income_range_vds == 'G: $75,000 - $99,999':
        return 87500.0
    if income_range_vds == 'H: $100,000 - $124,999':
        return 112500.0
    if income_range_vds == 'I: $125,000 - $149,999':
        return 137500.0
    if income_range_vds == 'J: $150,000 and over':
        return 175000.0
    return None


def get_networth(networthindicator_rollup):
    if networthindicator_rollup == 'A. Less than -$20,000':
        return -30000.0
    if networthindicator_rollup == 'B. -$20,000 thru -$2,500':
        return -11250.0
    if networthindicator_rollup == 'C. -$2,499 thru $2,499':
        return 0.0
    if networthindicator_rollup == 'D. $2,500 thru $24,999':
        return 13750.0
    if networthindicator_rollup == 'E. $25,000 thru $49,999':
        return 37500.0
    if networthindicator_rollup == 'F. $50,000 thru $74,999':
        return 62500.0
    if networthindicator_rollup == 'G. $75,000 thru $99,999':
        return 87500.0
    if networthindicator_rollup == 'H. $100,000 thru $149,999':
        return 125000.0
    if networthindicator_rollup == 'I. $150,000 thru $249,999':
        return 200000.0
    if networthindicator_rollup == 'J. $250,000 thru $499,999':
        return 375000.0
    if networthindicator_rollup == 'K. $500,000 thru $749,999':
        return 625000.0
    if networthindicator_rollup == 'L. $750,000 thru $999,999':
        return 875000.0
    if networthindicator_rollup == 'M. $1,000,000+':
        return 1250000.0
    return None


def get_length_of_residence(length_of_residence):
    if length_of_residence == '1':
        return 1.0
    if length_of_residence == '2':
        return 2.0
    if length_of_residence == '3':
        return 3.0
    if length_of_residence == '4':
        return 4.0
    if length_of_residence == '5':
        return 5.0
    if length_of_residence == '6':
        return 6.0
    if length_of_residence == '7':
        return 7.0
    if length_of_residence == '8':
        return 8.0
    if length_of_residence == '9':
        return 9.0
    if length_of_residence == '10':
        return 10.0
    if length_of_residence == '11':
        return 11.0
    if length_of_residence == '12':
        return 12.0
    if length_of_residence == '13':
        return 13.0
    if length_of_residence == '14':
        return 14.0
    if length_of_residence == '15+':
        return 25.0
    return None


def get_education(level_of_education):
    if level_of_education == 'A. Less than High School Diploma':
        return 1.0
    if level_of_education == 'B. High School Diploma':
        return 3.0
    if level_of_education == 'C. Some College':
        return 6.0
    if level_of_education == 'D. Bachelor Degree':
        return 10.0
    if level_of_education == 'E. Graduate Degree':
        return 15.0
    return None
