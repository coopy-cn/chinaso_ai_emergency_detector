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


if __name__ == "__main__":
    model = text_cnn()
    model.summary()