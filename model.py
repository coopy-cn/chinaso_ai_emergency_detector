#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/8/24 10:18
# @File    : model.py.py
# Copyright 2017 pureoym. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ========================================================================
from tensorflow.python.keras.layers import Dense, Flatten, Conv1D, MaxPooling1D, Dropout, Input, concatenate
from tensorflow.python.keras.layers import Embedding
from tensorflow.python.keras.models import Model

from tensorflow.python.keras.preprocessing import sequence
from tensorflow.python.keras.preprocessing.text import Tokenizer
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from tensorflow.python.keras.utils import to_categorical
from tensorflow.python.keras.initializers import Constant

import numpy as np
import os
import sys

BASE_DIR = "/application/search/ming"
WORD2VEC_DIR = os.path.join(BASE_DIR, 'sgns.merge.bigram')  # 词典地址
TEXT_DATA_DIR = os.path.join(BASE_DIR, '20_newsgroup')  # 数据地址
MAX_NUM_WORDS = 20000  # 取词汇表前20000个词
MAX_SEQUENCE_LENGTH = 1000  # 每篇文章最长词数
EMBEDDING_DIM = 300  # 词向量维数
FILTER_SIZES = [2, 3, 4, 5]  # 卷积核
DROPOUT_RATE = 0.5
VALIDATION_SPLIT = 0.2


# LEARNING_RATE = 0.001



def text_to_index_array(word2vec_dict, text):  # 文本转为索引数字模式
    new_sentences = []
    for sen in text:
        new_sen = []
        for word in sen:
            try:
                new_sen.append(word2vec_dict[word])  # 单词转索引数字
            except:
                new_sen.append(0)  # 索引字典里没有的词转为数字0
        new_sentences.append(new_sen)

    return np.array(new_sentences)


def pre_process():
    tokenizer = Tokenizer(nb_words=MAX_NB_WORDS)
    tokenizer.fit_on_texts(texts)
    sequences = tokenizer.texts_to_sequences(texts)

    word_index = tokenizer.word_index
    print('Found %s unique tokens.' % len(word_index))

    data = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)

    labels = to_categorical(np.asarray(labels))
    print('Shape of data tensor:', data.shape)
    print('Shape of label tensor:', labels.shape)

    # 切分成测试集和验证集
    indices = np.arange(data.shape[0])
    np.random.shuffle(indices)
    data = data[indices]
    labels = labels[indices]
    nb_validation_samples = int(VALIDATION_SPLIT * data.shape[0])

    x_train = data[:-nb_validation_samples]
    y_train = labels[:-nb_validation_samples]
    x_val = data[-nb_validation_samples:]
    y_val = labels[-nb_validation_samples:]
    return word_index, x_train, y_train, x_val, y_val


def get_embeddings_index():
    """
    加载预训练word2vec模型，返回字典embeddings_index
    :return: embeddings_index
    """
    embeddings_index = {}
    with open(WORD2VEC_DIR) as f:
        for line in f:
            values = line.split()
            word = values[0]
            coefs = np.asarray(values[1:], dtype='float32')
            embeddings_index[word] = coefs
    print('Found %s word vectors.' % len(embeddings_index))
    return embeddings_index


def get_texts_and_labels():
    """
    prepare text samples and their labels
    :return:
    """
    texts = []  # list of text samples
    labels_index = {}  # dictionary mapping label name to numeric id
    labels = []  # list of label ids
    for name in sorted(os.listdir(TEXT_DATA_DIR)):
        path = os.path.join(TEXT_DATA_DIR, name)
        if os.path.isdir(path):
            label_id = len(labels_index)
            labels_index[name] = label_id
            for fname in sorted(os.listdir(path)):
                if fname.isdigit():
                    fpath = os.path.join(path, fname)
                    args = {} if sys.version_info < (3,) else {'encoding': 'utf-8'}
                    with open(fpath, **args) as f:
                        t = f.read()
                        i = t.find('\n\n')  # skip header
                        if 0 < i:
                            t = t[i:]
                        texts.append(t)
                    labels.append(label_id)
    print('Found %s texts.' % len(texts))
    return texts, labels_index, labels


def get_word_index(texts, labels):
    """
    vectorize the text samples into a 2D integer tensor
    :param texts:
    :return:
    """
    tokenizer = Tokenizer(num_words=MAX_NUM_WORDS)
    tokenizer.fit_on_texts(texts)
    sequences = tokenizer.texts_to_sequences(texts)

    word_index = tokenizer.word_index
    print('Found %s unique tokens.' % len(word_index))

    data = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)
    labels = to_categorical(np.asarray(labels))

    print('Shape of data tensor:', data.shape)
    print('Shape of label tensor:', labels.shape)
    return word_index, labels


def get_training_set_and_validation_set(data, labels):
    """
    split the data into a training set and a validation set
    :return:
    """
    indices = np.arange(data.shape[0])
    np.random.shuffle(indices)
    data = data[indices]
    labels = labels[indices]
    num_validation_samples = int(VALIDATION_SPLIT * data.shape[0])

    x_train = data[:-num_validation_samples]
    y_train = labels[:-num_validation_samples]
    x_val = data[-num_validation_samples:]
    y_val = labels[-num_validation_samples:]

    return x_train, y_train, x_val, y_val


def get_embedding_matrix(embeddings_index, word_index):
    """
    prepare embedding matrix
    使用embeddings_index，word_index生成预训练矩阵embedding_matrix。
    :param embeddings_index:
    :param word_index:
    :return:
    """
    num_words = min(MAX_NUM_WORDS, len(word_index) + 1)
    embedding_matrix = np.zeros((num_words, EMBEDDING_DIM))
    for word, i in word_index.items():
        if i >= MAX_NUM_WORDS:
            continue
        embedding_vector = embeddings_index.get(word)
        if embedding_vector is not None:
            # words not found in embedding index will be all-zeros.
            embedding_matrix[i] = embedding_vector
    return embedding_matrix


def text_cnn():
    """
    构建text_cnn模型
    :return:
    """
    # Inputs
    sequence_input = Input(shape=(MAX_SEQUENCE_LENGTH,), dtype='int32')

    # Embeddings layers
    # load pre-trained word embeddings into an Embedding layer
    # note that we set trainable = False so as to keep the embeddings fixed
    embeddings_index = get_embeddings_index()
    word_index = get_word_index()
    num_words = min(MAX_NUM_WORDS, len(word_index) + 1)
    embedding_matrix = get_embedding_matrix(embeddings_index, word_index)
    embedding_layer = Embedding(num_words,
                                EMBEDDING_DIM,
                                embeddings_initializer=Constant(embedding_matrix),
                                input_length=MAX_SEQUENCE_LENGTH,
                                trainable=False)

    # embeddings_index = get_embeddings_index()
    # word_index = get_word_index()
    # embedding_matrix = get_embedding_matrix(embeddings_index, word_index)
    # embedding_layer = Embedding(len(word_index) + 1,
    #                             EMBEDDING_DIM,
    #                             weights=[embedding_matrix],
    #                             input_length=MAX_SEQUENCE_LENGTH,
    #                             trainable=False)

    embedded_sequences = embedding_layer(sequence_input)

    # conv layers
    convs = []
    for fs in FILTER_SIZES:
        l_conv = Conv1D(filters=100, kernel_size=fs, activation='relu')(embedded_sequences)
        l_pool = MaxPooling1D(MAX_SEQUENCE_LENGTH - fs + 1)(l_conv)
        l_pool = Flatten()(l_pool)
        convs.append(l_pool)
    merge = concatenate(convs, axis=1)

    x = Dropout(DROPOUT_RATE)(merge)
    x = Dense(32, activation='relu')(x)

    preds = Dense(units=1, activation='sigmoid')(x)

    model = Model(sequence_input, preds)
    model.compile(loss="categorical_crossentropy",
                  optimizer="rmsprop",
                  metrics=['acc'])

    return model


# def text_cnn(maxlen=150, max_features=2000, embed_size=32):
#     # Inputs
#     comment_seq = Input(shape=[maxlen], name='x_seq')
#
#     # Embeddings layers
#     emb_comment = Embedding(max_features, embed_size)(comment_seq)
#
#     # conv layers
#     convs = []
#     filter_sizes = [2, 3, 4, 5]
#     for fsz in filter_sizes:
#         l_conv = Conv1D(filters=100, kernel_size=fsz, activation='relu')(emb_comment)
#         l_pool = MaxPooling1D(maxlen - fsz + 1)(l_conv)
#         l_pool = Flatten()(l_pool)
#         convs.append(l_pool)
#     merge = concatenate(convs, axis=1)
#
#     out = Dropout(0.5)(merge)
#     output = Dense(32, activation='relu')(out)
#
#     output = Dense(units=1, activation='sigmoid')(output)
#
#     model = Model([comment_seq], output)
#     #     adam = optimizers.Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
#     model.compile(loss="binary_crossentropy", optimizer="adam", metrics=['accuracy'])
#
#     return model


def preprocessing(train_texts, train_labels, test_texts, test_labels):
    tokenizer = Tokenizer(num_words=2000)  # 建立一个2000个单词的字典
    tokenizer.fit_on_texts(train_texts)
    # 对每一句影评文字转换为数字列表，使用每个词的编号进行编号
    x_train_seq = tokenizer.texts_to_sequences(train_texts)
    x_test_seq = tokenizer.texts_to_sequences(test_texts)
    x_train = sequence.pad_sequences(x_train_seq, maxlen=150)
    x_test = sequence.pad_sequences(x_test_seq, maxlen=150)
    y_train = np.array(train_labels)
    y_test = np.array(test_labels)
    return x_train, y_train, x_test, y_test


if __name__ == "__main__":
    x_train, y_train, x_val, y_val = get_training_set_and_validation_set()
    train_texts, train_labels = read_files('train')
    test_texts, test_labels = read_files('test')
    x_train, y_train, x_test, y_test = preprocessing(train_texts, train_labels, test_texts, test_labels)
    model = text_cnn()
    model.summary()
    batch_size = 64
    epochs = 10
    model.fit(x_train, y_train,
              batch_size=batch_size,
              epochs=10,
              validation_data=(x_val,y_val),
              shuffle=True)
    scores = model.eveluate(x_test, y_test)
    print('test_loss: %f, accuracy: %f' % (scores[0], scores[1]))
