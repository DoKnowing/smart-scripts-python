# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)

from operator import *
import json
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.image as mlim
from PIL import Image
import numpy
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors, DenseVector
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.linalg import Matrix
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.regression import LabeledPoint
# 线性回归
from pyspark.mllib.regression import LinearRegressionWithSGD
# 决策树回归
from pyspark.mllib.tree import DecisionTree


def t1():
    sc = SparkContext(master='local[3]', appName='History')
    data = sc.textFile(u'C:\\Users\\SMA\Desktop\\数据中心\\tmp\\history.csv', 1) \
        .map(lambda line: t2(line, ',')).map(lambda t: (t[0], t[1], t[2]))

    # 购买次数
    # numPurchses = data.count()
    # print u'购买次数: ' + numPurchses

    # 多少个不同用户购买商品
    # uniqueUsers = data.map(lambda user: user[0]).distinct().count()
    # print u'用户个数: ' + str(uniqueUsers)

    # 购买的收入总数
    # totalRevenue = data.map(lambda user: float(user[2])).sum()
    # print u'收入总数: ' + str(totalRevenue)

    # 最热销的商品是什么
    popularProduct = data.map(lambda user: (user[1], 1)).reduceByKey(add).sortBy(lambda x: x[1],
                                                                                 ascending=False).collect()
    print u'商品消费排行: %s' % (popularProduct)


def t2(line, separator='\t'):
    return line.split(separator)


def filter_id(user_id, other):
    if user_id == other:
        return True
    else:
        return False


def cosine_similarity(v1, v2):
    """
    dot: 向量的点积
    norm(p): 模长,p表示范式
    :param v1: Vectors
    :param v2: Vectors
    :return:
    """
    return v1.dot(v2) / (v1.norm(2) * v2.norm(2))


def map_similarity(pair, v2):
    """
    计算余弦值
    :param pair:
    :param v2:
    :return:
    """
    v1 = Vectors.parse(str(pair[1].tolist()))
    return pair[0], cosine_similarity(v1, v2)


def user_data():
    sc = SparkContext(master='local[3]', appName='History')
    # userId|age|gender|职业|邮编
    data = sc.textFile(u'D:\\tmp\\data\\movies\\ml-100k\\u.user').map(lambda line: t2(line, '|'))

    # 统计用户数
    # userNum = data.map(lambda user: user[0]).distinct().count()
    # print u'共计用户: %d 个' % userNum

    # 统计年龄分布
    # ageDist = data.map(lambda user: int(user[1])).collect()
    # print u'年龄分布: %s ' % ageDist

    # 统计性别分布
    # genderNum = data.map(lambda user: (user[2], 1)).reduceByKey(lambda a, b: a + b).map(lambda x: x[1]).collect()
    # print u'性别分布: %s' % genderNum

    # 统计职业:
    # occupationsNum = data.map(lambda user: user[3]).distinct().count()
    # print u'职业种类: %d' % occupationsNum

    # 邮编分布
    # codeNum = data.map(lambda user: user[4]).distinct().count()
    # print u'邮编种类: %d' % codeNum

    # 年龄分布图
    # plt.hist(ageDist, 20, normed=True, color='blue')
    # fig = plt.gcf()
    # fig.set_size_inches(16, 10)
    # plt.show()

    count_by_occupations = data.map(lambda user: (user[3], 1)).reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=True).collect()

    x_axis1 = []
    y_axis1 = []
    for c in count_by_occupations:
        x_axis1.append(c[0])
        y_axis1.append(c[1])

    # x_axis = x_axis1[numpy.argsort(y_axis1)]
    # y_axis = y_axis1[numpy.argsort(y_axis1)]
    pos = numpy.arange(len(count_by_occupations))
    width = 1.0
    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))
    ax.set_xticklabels(x_axis1)

    plt.bar(pos, y_axis1, width, color='lightblue')
    plt.xticks(rotation=30)
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.show()


def mc_data():
    """
    最小二乘法实现推荐算法
    :return:
    """
    # 推荐算法
    sc = SparkContext(master='local[3]', appName='ALS')
    # userId|moviesId|星级评价|时间戳
    data = sc.textFile(u'D:\\tmp\\data\\movies\\ml-100k\\u.data').map(lambda line: t2(line, '\t'))
    model = ALS.train(data.map(lambda x: (int(x[0]), int(x[1]), int(x[2]))), rank=50, iterations=10, lambda_=0.01)

    # 预测
    # predicteRating = model.predict(789, 123)
    # print 'predicteRating=%f' % predicteRating

    # 返回得分最高(推荐)的前K=10个
    # recommends = model.recommendProducts(789, 10)
    # print 'topK : %s' % recommends

    # 校验推荐算法
    # movies = data.map(lambda x: (int(x[0]), int(x[1]), int(x[2]))).keyBy(lambda x: x[0]).lookup(789)
    # print '789评价的电影数量: %d' % len(movies)
    # movies_topK = data.map(lambda x: (int(x[0]), int(x[1]), int(x[2]))).filter(lambda x: filter_id(x[0], 789)) \
    #     .sortBy(lambda x: x[2], ascending=False).take(10)
    # print '789评价的电影(Top10): %s' % movies_topK

    # 结论,然而没有一个准的


    # 计算物品的余弦相似度
    itemFactor = model.productFeatures().lookup(567)[0]
    itemVector = Vectors.parse(str(itemFactor.tolist()))

    cosine = model.productFeatures().map(lambda x: map_similarity(x, itemVector)) \
        .sortBy(lambda x: x[1], ascending=False).collect()
    print cosine


def squared_error(actual, pred):
    """
    平方误差
    :param actual:
    :param pred:
    :return:
    """
    return (actual - pred) ** 2


def abs_error(actual, pred):
    """
    平均绝对误差
    :param actual:
    :param pred:
    :return:
    """
    return numpy.abs(actual - pred)


def squared_log_error(actual, pred):
    """
    均方根对数误差
    :param actual:
    :param pred:
    :return:
    """
    return (numpy.log(pred + 1) - numpy.log(actual + 1)) ** 2


def evaluate(train, test, iterations, step, regParam, regType, intercept):
    model = LinearRegressionWithSGD.train(train, iterations=iterations, step=step, regParam=regParam, regType=regType,
                                          intercept=intercept)
    tp = test.map(lambda p: (p.label, model.predict(p.features)))
    rmsle = numpy.sqrt(tp.map(lambda (t, p): squared_log_error(t, p)).mean())
    return rmsle


def get_mapping(rdd, idx):
    return rdd.map(lambda x: x[idx]).distinct().zipWithIndex().collectAsMap()


def extract_features(record, cat_len, mappings):
    cat_vec = numpy.zeros(cat_len)
    i = 0
    step = 0
    for field in record[2:9]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i += 1
        step += len(m)
    num_vec = numpy.array([float(field) for field in record[10:14]])
    return numpy.concatenate((cat_vec, num_vec))


def extract_label(record):
    return float(record[-1])


def extract_features_dt(record):
    return numpy.array(map(float, record[2:14]))


def regression_example(sc, local_path):
    # sc = SparkContext(master='local[3]', appName='REGRESSION_DATA')
    data = sc.textFile(local_path).map(lambda line: t2(line, ','))

    mappings = [get_mapping(data, i) for i in range(2, 10)]

    cat_len = sum(map(len, mappings))
    num_len = len(data.first()[11:15])
    total_len = num_len + cat_len
    print 'cat=%d ,num=%d ,total=%d' % (cat_len, num_len, total_len)
    return mappings, cat_len


def regression():
    """
    回归算法
    :return:
    """
    day_file = u'D:\\tmp\\data\\spark\\regression\\Bike-Sharing-Dataset\\day.csv'
    hour_file = u'D:\\tmp\\data\\spark\\regression\\Bike-Sharing-Dataset\\hour.csv'

    sc = SparkContext(master='local[3]', appName='REGRESSION')

    mappings, cat_len = regression_example(sc=sc, local_path=hour_file)

    data = sc.textFile(hour_file).map(lambda line: t2(line, ','))

    ### 线性回归
    example = data.map(
        lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len=cat_len, mappings=mappings)))
    # example = data.map(
    #     lambda r: LabeledPoint(numpy.log(extract_label(r)), extract_features(r, cat_len=cat_len, mappings=mappings)))
    # linear_mode = LinearRegressionWithSGD.train(example, iterations=10, step=0.1, intercept=False)
    # true_vs_predicted = example.map(lambda x: (x.label, linear_mode.predict(x.features)))
    # 由于模型取了对数,所以最后的结果也需要取指数
    # true_vs_predicted = example.map(lambda x: (numpy.exp(x.label), numpy.exp(linear_mode.predict(x.features))))
    # print 'Linear Mode Predictions : %s' % true_vs_predicted.collect()[0:10]

    # mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
    # mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
    # rmsle = true_vs_predicted.map(lambda (t, p): squared_log_error(t, p)).mean()

    # print 'Linear Mode - Mean Squared Error(均方误差): %f' % mse
    # print 'Linear Mode - Mean Absolute Error(平均绝对误差): %f' % mae
    # print 'Linear Mode - Root Mean Squared Log Error(均方根误差): %f' % rmsle

    ### 决策树回归
    # example_dt = data.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    # dt_model = DecisionTree.trainRegressor(example_dt, {})
    # preds = dt_model.predict(example_dt.map(lambda x: x.features))
    # actual = example.map(lambda x: x.label)
    # true_vs_predicted = actual.zip(preds)
    # print 'DecisionTree Mode Predictions : %s' % true_vs_predicted.collect()[0:10]

    # mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
    # mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
    # rmsle = true_vs_predicted.map(lambda (t, p): squared_log_error(t, p)).mean()
    #
    # print 'DecisionTree Mode - Mean Squared Error(均方误差): %f' % mse
    # print 'DecisionTree Mode - Mean Absolute Error(平均绝对误差): %f' % mae
    # print 'DecisionTree Mode - Root Mean Squared Log Error(均方根误差): %f' % rmsle

    ### 模型参数调优,训练模型
    data_with_idx = example.zipWithIndex().map(lambda (k, v): (v, k))
    test = data_with_idx.sample(False, 0.2, 42)
    # 取x中不包含y的key x.subtractByKey(y)
    train = data_with_idx.subtractByKey(test)

    train_data = train.map(lambda (idx, p): p)
    test_data = test.map(lambda (idx, p): p)
    # print 'train data size: %d' % train_data.count()
    # print 'test data size: %d' % test_data.count()

    ## 调优
    # 迭代次数
    iterations = [1, 5, 10, 20, 50, 100]
    metrics = [evaluate(train_data, test_data, it, 0.01, 0.0, 'l2', False) for it in iterations]
    print iterations
    print metrics

    plt.plot(iterations, metrics)
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.xscale('log')
    plt.show()


def optimize_regression():
    day_file = u'D:\\tmp\\data\\spark\\regression\\Bike-Sharing-Dataset\\day.csv'
    hour_file = u'D:\\tmp\\data\\spark\\regression\\Bike-Sharing-Dataset\\hour.csv'

    sc = SparkContext(master='local[3]', appName='REGRESSION')
    data = sc.textFile(hour_file).map(lambda line: t2(line, ','))

    # 查看数据分布图
    # targets = data.map(lambda x: float(x[-1])).collect()
    # 取对数
    # targets = data.map(lambda x: numpy.log(float(x[-1]))).collect()
    # 取平方根
    targets = data.map(lambda x: numpy.sqrt(float(x[-1]))).collect()
    n, bins, patches = plt.hist(targets, bins=100, edgecolor='black', facecolor='lightblue', normed=True)
    y = mlab.normpdf(bins, 100, 15)
    plt.plot(bins, y, 'r--')
    plt.xlabel('Smarts')
    plt.ylabel('Probability')
    plt.title(r'Histogram of IQ: $\mu=100$, $\sigma=15$')
    fig = plt.gcf()
    fig.set_size_inches(16, 10)
    plt.show()


def data_map(movie, genre_map):
    genres = movie[5:len(movie)]
    assigned = []
    post = 0
    for g in genres:
        if int(g) == 1:
            assigned.extend([genre_map[str(post)]])
        post += 1
    return movie[0], (movie[1], assigned)


def compute_distance(v1, v2):
    return (v1 - v2).norm(2)


def titel_factor(record, movie_clustring_mode):
    pred = movie_clustring_mode.predict(record[1][1])
    cluster_center = movie_clustring_mode.clusterCenters
    dist = compute_distance(DenseVector(cluster_center), record[1][1])
    ## 编号| 电影名|标签|分类|距离
    return record[0], record[1][0][0], record[1][0][1], pred, dist


def clustering():
    sc = SparkContext(master='local[3]', appName='CLUSTERING')
    movies = sc.textFile(u'D:\\tmp\\data\\movies\\ml-100k\\u.item').map(lambda line: t2(line, '|'))

    genres = sc.textFile(u'D:\\tmp\\data\\movies\\ml-100k\\u.genre')
    genre_map = genres.filter(lambda x: x) \
        .map(lambda line: t2(line, '|')).map(lambda x: (x[1], x[0])).collectAsMap()

    titlesAndGenres = movies.map(lambda x: data_map(x, genre_map))
    # print titlesAndGenres.take(10)

    # 训练推荐模型
    raw_ratings = sc.textFile(u'D:\\tmp\\data\\movies\\ml-100k\\u.data').map(lambda line: t2(line, '\t')).map(
        lambda x: x[0:3])
    ratings = raw_ratings.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
    # ratings.cache()
    als_model = ALS.train(ratings, 50, 10, 0.1)

    movies_factors = als_model.productFeatures().map(lambda (x, y): (x, Vectors.dense(y)))
    movies_vectors = movies_factors.map(lambda x: x[1])

    # user_factors = als_model.userFeatures().map(lambda (x, y): (x, Vectors.dense(y)))
    # user_vectors = user_factors.map(lambda x: x[1])

    # 归一化
    # movie_matrix = RowMatrix(movies_vectors)
    num_clusters = 5
    num_iterations = 10
    num_runs = 3
    # K-mean训练
    movies_clustering_model = KMeans.train(movies_vectors, num_clusters, maxIterations=num_iterations, runs=100)
    # user_clustering_model = KMeans.train(user_vectors, num_clusters, maxIterations=num_iterations, runs=num_runs)
    ## 分类
    title_and_factors = titlesAndGenres.map(lambda x: (int(x[0]), x[1])).join(movies_factors)
    cls = title_and_factors.map(lambda r: titel_factor(r, movies_clustering_model)).map(lambda x: (x[3], x)) \
        .groupByKey().collect()

    for c in cls:
        print 'Cluster %s' % c[0]
        for m in c[1]:
            print 'Movies: %s ,Label: %s ,Score: %f' % (m[1], m[2], m[4])
            # print cls


def format_path(path):
    return path.replace('file:/', '')


def pixels_image(image_path, width=50, height=50, type='JPG'):
    img = Image.open(image_path)
    img = img.convert('L')  # 转为灰白图
    img.thumbnail((width, height))  # 截取
    return numpy.array(img)


def data_reduce_dimesion():
    """
    数据降维,2.2.0才支持
    :return:
    """
    sc = SparkContext(master='local[3]', appName='REDUCE_DIMESION')
    ## 注意路径
    data = sc.wholeTextFiles(path=u'D:/tmp/data/spark/lfw/*/*')
    pixels = data.map(lambda x: format_path(x[0])).map(lambda x: pixels_image(x, 100, 100))
    array_vector = pixels.map(lambda x: x.flatten())
    vectors = array_vector.map(lambda p: Vectors.dense(p))
    vectors.persist()
    # 标准化,正则化
    scaler = StandardScaler(withStd=True, withMean=False)
    model = scaler.fit(vectors)
    scaler_vectors = model.transform(vectors)
    maxtrix = RowMatrix(scaler_vectors)
    #  2.2.0才有该函数,计算前N个主成分
    pc = maxtrix.computePrincipalComponents(10)
    print pc


if __name__ == "__main__":
    # user_data()
    # mc_data()
    # regression()
    # optimize_regression()
    # clustering()
    data_reduce_dimesion()
