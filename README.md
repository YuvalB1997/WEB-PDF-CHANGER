
## Authors:
Yuval Buzhor,
Yaniv Krol



## How to run:
1. Create an S3 bucket named "yuvalmapreduce".
2. Upload the following jar files to the bucket:
    - step-count.jar
    - step-join.jar
    - step-calculcate.jar
    - step-sort.jar
3. Run Main: java -jar main.jar
## Job Flow Description:
### Count step:
#### Mapper

For each 3gram `<w1, w2, w3>` and its corresponding appearances `<count>`,
creates the key-values pairs  
`<w1, count>`, `<w1 w2, count>`, `<w1 w2 w3>` count.  
Another special key-values pair `<*C0*, count>` is created to count the total number of word instances. 
For each of these pairs, if any of the characters is not alphabetic, the pair isn't omitted.

#### Combiner

Performs local reduction of the countings.

#### Reducer

Same as the combiner with the excpetion that if the key is `*C0*` then the redution result is uploaded
to a temp file in S3 to be later used in the calculation step. 

### Join step:

This step performs reducer side join which doesn't use memory.

#### Mapper

If the key is a 1gram or 2gram, the key is sent with added `#` mark
and the value is the count, i.e. `<w1#, count>` or `<w1 w2#, count>`.  
Else, we are dealing with a 3 gram. We create the following keys:
`<w2$, w1 w2 w3 count>, <w3$, w1 w2 w3 count>, <w1 w2$ w1 w2 w3 count>, <w2 w3$, w1 w2 w3 count>`

* If w2 equals w3 then the key is only sent once, same goes for "w1 w2" and "w2 w3".

#### Reducer

Performs the join operation.

If the key ends with a `#` (1/2-gram) then we know by lexicographical order that the next key that ends with a`$` holds 
all the 3grams that contain this 1/2-gram. So the count of that 1/2-gram is saved until a`$` key appears.
When the corresponding `$` key arrives (Which is not guaranteed to be immediately), 
For each of the 3-grams in the values list, a key-value pair `<w1 w2 w3, C1 N1 C2 N2 N3>` is created
where C1 N1 C2 N2 are only populated if the current key matches this 1/2-gram, if not then it defaults to `-1`.
N3 is known a priori and its values is always populated. 
It's guaranteed that these keys will go to the same reducer by the implemeted partitioner that uses the hashcode 
of the keys without the `#` or `$` postfix.

Example: 

For the 3-gram `<a b c, 7>` and the 2-gram `<a b, 10>`, First the key-value pair `<a b#, 10>` arrives and saved in memory.
The next key that ends with `$` will be `<a b$, a b c 7>` and the output will be `<a b c, -1 -1 10 -1 7>`.

### Calculcate step:

This step performs the calculation and for each 3-gram outputs the probability p=P(w3|w1w2).

#### Mapper

The mapper is the id mapper.

#### Reducer

In the setup, loads the value of C0 from S3 (was calculcated in the counting step).

When a key (3-gram) arrives to the reducer, its values hold all the needed information to perform the operation.

Example:
```
one two three	11 -1 -1 -1 7
one two three	-1 8 -1 -1 7
one two three	-1 -1 12 -1 7
one two three	-1 -1 -1 14 7
```
`two` (C1) appears 11 times  
`three` (N1) appears 8 times  
`one two` (C2) appears 12 times  
`two three` (C2) appears 14 times  
`one two three` (N3) appears 7 times  

The reducer extract this information and caulcuates the probability p as described here: https://dl.acm.org/citation.cfm?id=1034712.

### Sort step:

This step sorts the output in the following order, and outputs it to a single file:
1. By w1 w2, ascending
2. By the probability for w3, descending.


## Results

### Traffic

We ran the job with 8 instances (1 master and 7 workers), once with combiner and once without 
(remember that we only have combiner in the counting step).

Here are the total nubmer of values ("Reduce input records" in syslog):

|   | count | join | calculcate | sort | total |
| - | ----- | ---- | ---------- | ---- | ----- |
| With combiner | 3632806 | 7604398 | 6742646 | 1686118 | 19665968 |
| Without combiner | 358096675 | 7604398 | 6742646 | 1686118 | 374129837 |

### Ineteresting results

Here are 10 interesting word pairs and their top 5 likely third word:

פושעי ישראל מלאים	0.14438923986353486  
פושעי ישראל בגופן	0.14165208670387242  
פושעי ישראל אין	0.09517695327013075  
פושעי ישראל בגופם	0.07689607596593892  
פושעי ישראל שיחזרו	0.06660934540897513  

אבא לא היה	0.11399717557300883  
אבא לא היו	0.07696865145320596  
אבא לא ידע	0.07670571892164038  
אבא לא רצה	0.06561797351373042  
אבא לא מזכי	0.060662129863543364  

הבית היה מלא	0.08898841573799447  
הבית היה ריק	0.060838887393133034  
הבית היה מוקף	0.048124164694180786  
הבית היה בן	0.04633773639598993  
הבית היה בית	0.04533056607060931  

כאן גם את	0.06294612319960607  
כאן גם על	0.04892206421691961  
כאן גם כן	0.04749473305026122  
כאן גם המקום	0.036836273185014805  
כאן גם כמה	0.03497863122844989  

של אגף העתיקות	0.193710892606909  
של אגף זה	0.17046146122479786  
של אגף החינוך	0.12198660335831067  
של אגף התכנון	0.11173604108371507  
של אגף העבודה	0.1031713838874226  

באוניברסיטאות במערב והשלכותיהן	0.9190818116491181  
באוניברסיטאות של גרמניה	0.2536071622171511  
באוניברסיטאות של ברלין	0.23676529529633022  
באוניברסיטאות של אירופה	0.22899811257916666  
באוניברסיטאות של ארצות	0.20326426438353407  

לחול אלא על	0.9257231329728011  
לחול בין אור	0.9260852577782136  
לחול במחולות ויצאתם	0.9532094396801039  
לחול גם על	0.9318388121230465  
לחול ואחד לשבת	0.9237423295380546  

היא לבשה את	0.15910913478829544  
היא לבשה שמלה	0.12495634598305472  
היא לבשה צורה	0.1096759735457592  
היא לבשה מעיל	0.07231361318144093  
היא לבשה חולצה	0.07004048127592961  

מה שאתה רוצה	0.04299850639019441  
מה שאתה רואה	0.04116860942404927  
מה שאתה עושה	0.038988388229327715  
מה שאתה מוציא	0.0381162434616895  
מה שאתה יכול	0.037133693540678366  

תתקבל החלטה על	0.9286955580998937  
תתקבל הצעה זו	0.9569202932643147  
תתקבל התמונה הבאה	0.9266506335790688  
תתקבל על דעת	0.19034033271879258  
תתקבל על ידי	0.1468280386286567  
