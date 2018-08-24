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


def text_cnn(maxlen=150, max_features=2000, embed_size=32):
    # Inputs
    comment_seq = Input(shape=[maxlen], name='x_seq')

    # Embeddings layers
    emb_comment = Embedding(max_features, embed_size)(comment_seq)

    # conv layers
    convs = []
    filter_sizes = [2, 3, 4, 5]
    for fsz in filter_sizes:
        l_conv = Conv1D(filters=100, kernel_size=fsz, activation='relu')(emb_comment)
        l_pool = MaxPooling1D(maxlen - fsz + 1)(l_conv)
        l_pool = Flatten()(l_pool)
        convs.append(l_pool)
    merge = concatenate(convs, axis=1)

    out = Dropout(0.5)(merge)
    output = Dense(32, activation='relu')(out)

    output = Dense(units=1, activation='sigmoid')(output)

    model = Model([comment_seq], output)
    #     adam = optimizers.Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(loss="binary_crossentropy", optimizer="adam", metrics=['accuracy'])

    return model


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
    train_texts, train_labels = read_files('train')
    test_texts, test_labels = read_files('test')
    x_train, y_train, x_test, y_test = preprocessing(train_texts, train_labels, test_texts, test_labels)
    model = text_cnn()
    model.summary()
    batch_size = 64
    epochs = 10
    model.fit(x_train,y_train,
              validation_split = 0.1,
              batch_size = batch_size,
              shuffle = True)
    scores = model.eveluate(x_test,y_test)
    print('test_loss: %f, accuracy: %f' % (scores[0],scores[1]))