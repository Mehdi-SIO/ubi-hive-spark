#!pip install pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row

sc = SparkContext()
sqlContext = SQLContext(sc)
l = [(16130, '2019-11-10', '2019-11-10 20:59:35', '76 rue de Valmy', '64.67.38.57', 87),
     (76280, '2019-12-12', '2019-12-12 18:4:49', '46 rue Robespierre', '45.10.45.62', 58),
     (14469, '2019-12-5', '2019-12-5 16:18:11', '58 rue Raspail', '18.46.75.72', 123),
     (55433, '2019-11-24', '2019-11-24 23:48:19', '53 rue Raspail', '5.59.35.7', 124),
     (67310, '2019-11-12', '2019-11-12 16:0:59', '60 rue Robespierre', '69.32.4.64', 34),
     (80959, '2019-12-20', '2019-12-20 14:6:46', '23 rue Raspail', '22.19.65.69', 136),
     (38510, '2019-10-20', '2019-10-20 13:42:35', '52 rue Robespierre', '78.53.39.3', 25),
     (77224, '2019-12-20', '2019-12-20 13:49:4', '41 rue Raspail', '56.28.17.37', 93),
     (88075, '2019-10-20', '2019-10-20 3:51:3', '78 rue de Valmy', '59.32.56.42', 138),
     (84461, '2019-12-8', '2019-12-8 13:29:15', '31 rue de Paris', '77.48.21.53', 139),
     (10959, '2019-12-16', '2019-12-16 16:46:49', '75 rue Etienne Marcel', '56.80.75.64', 65),
     (63656, '2019-12-20', '2019-12-20 14:56:0', '24 rue de Valmy', '74.47.54.29', 63),
     (74587, '2019-10-16', '2019-10-16 2:32:47', '45 rue de la République', '65.54.40.29', 18),
     (73621, '2019-12-19', '2019-12-19 15:49:1', '50 rue Barbes', '66.4.40.69', 147),
     (93224, '2019-10-13', '2019-10-13 22:12:50', '26 rue Marceau', '33.69.79.59', 36),
     (72946, '2019-11-23', '2019-11-23 20:7:51', '1 rue Paul Eluard', '32.80.19.31', 121),
     (34752, '2019-11-16', '2019-11-16 12:46:1', '17 rue Barbes', '69.30.67.17', 134),
     (33948, '2019-10-4', '2019-10-4 11:25:1', '25 rue Lavoisier', '12.77.49.61', 104),
     (59241, '2019-10-15', '2019-10-15 1:4:4', '39 rue Marceau', '64.63.56.6', 120),
     (91393, '2019-11-5', '2019-11-5 7:46:5', '3 rue Garibaldi', '70.8.65.31', 82),
     (18502, '2019-11-15', '2019-11-15 17:45:15', '10 rue Robespierre', '14.8.66.78', 21),
     (96896, '2019-12-6', '2019-12-6 5:57:52', '15 rue Garibaldi', '16.74.53.38', 76),
     (96637, '2019-11-14', '2019-11-14 6:53:49', '35 rue Etienne Marcel', '10.17.4.52', 55),
     (11783, '2019-10-14', '2019-10-14 10:28:28', '73 rue Emile Zola', '51.25.26.43', 34),
     (88023, '2019-11-8', '2019-11-8 16:19:27', '23 rue de la République', '60.45.74.44', 109),
     (76892, '2019-10-19', '2019-10-19 11:34:6', '13 rue Paul Eluard', '57.63.80.14', 111),
     (49534, '2019-12-19', '2019-12-19 5:54:23', '11 rue Lavoisier', '12.62.74.46', 25),
     (41402, '2019-12-4', '2019-12-4 19:36:18', '21 rue Emile Zola', '26.37.38.5', 67),
     (18876, '2019-10-23', '2019-10-23 12:3:42', '50 rue Lavoisier', '15.16.57.28', 21),
     (29221, '2019-10-21', '2019-10-21 20:0:20', '3 rue du Progres', '28.16.4.15', 58),
     (88126, '2019-11-13', '2019-11-13 1:5:28', '26 rue du Progres', '29.8.17.24', 97),
     (16350, '2019-12-01', '2019-12-01 17:28:18', '23 rue Marceau', '40.75.21.79', 48),
     (88832, '2019-12-12', '2019-12-12 9:32:18', '53 rue Paul Eluard', '79.73.1.57', 68),
     (35803, '2019-11-7', '2019-11-7 24:22:25', '63 rue Etienne Marcel', '26.75.30.70', 101),
     (34752, '2019-12-12', '2019-12-12 15:55:30', '17 rue Barbes', '69.30.67.17', 90),
     (33948, '2019-10-20', '2019-10-20 24:51:49', '25 rue Lavoisier', '12.77.49.61', 142),
     (59241, '2019-12-6', '2019-12-6 8:27:44', '39 rue Marceau', '64.63.56.6', 91),
     (91393, '2019-12-16', '2019-12-16 17:7:5', '3 rue Garibaldi', '70.8.65.31', 141),
     (18502, '2019-11-13', '2019-11-13 1:29:45', '10 rue Robespierre', '14.8.66.78', 38),
     (96896, '2019-12-21', '2019-12-21 18:33:23', '15 rue Garibaldi', '16.74.53.38', 53),
     (96637, '2019-12-7', '2019-12-7 18:15:33', '35 rue Etienne Marcel', '10.17.4.52', 79),
     (11783, '2019-10-7', '2019-10-7 21:14:9', '73 rue Emile Zola', '51.25.26.43', 145),
     (88023, '2019-10-3', '2019-10-3 10:51:55', '58 rue Paul Eluard', '60.45.74.44', 48),
     (76892, '2019-12-1', '2019-12-1 10:57:23', '13 rue Paul Eluard', '45.47.58.13', 112),
     (49534, '2019-11-7', '2019-11-7 9:3:47', '25 rue Lebour', '47.23.26.30', 26),
     (16888, '2019-12-04', '2019-12-04 19:30:35', '21 rue Emile Zola', '26.37.38.5', 102)
     ]
rdd = sc.parallelize(l)
transaction = rdd.map(
    lambda x: Row(customerid=x[0], order_date=x[1], order_datetime=x[2], address=x[3], ip_address=x[4],
                  amount_eur=x[5]))
schemaTransaction = sqlContext.createDataFrame(transaction)
