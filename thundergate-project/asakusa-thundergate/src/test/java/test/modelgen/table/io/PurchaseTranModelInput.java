/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test.modelgen.table.io;
import java.io.IOException;

import javax.annotation.Generated;

import test.modelgen.table.model.PurchaseTran;

import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.RecordParser;
/**
 * TSVファイルなどのレコードを表すファイルを入力として{@link PurchaseTran}を読み出す。
 */
@Generated("ModelInputEmitter:0.0.1")@SuppressWarnings("deprecation") public final class PurchaseTranModelInput
        implements ModelInput<PurchaseTran> {
    /**
     * 内部で利用するパーサー
     */
    private final RecordParser parser;
    /**
     * インスタンスを生成する。
     * @param parser 利用するパーサー
     * @throws IllegalArgumentException 引数にnullが指定された場合
     */
    public PurchaseTranModelInput(RecordParser parser) {
        if(parser == null) {
            throw new IllegalArgumentException();
        }
        this.parser = parser;
    }
    @Override public boolean readTo(PurchaseTran model) throws IOException {
        if(parser.next()== false) {
            return false;
        }
        parser.fill(model.getSidOption());
        parser.fill(model.getVersionNoOption());
        parser.fill(model.getRgstDatetimeOption());
        parser.fill(model.getUpdtDatetimeOption());
        parser.fill(model.getPurchaseNoOption());
        parser.fill(model.getPurchaseTypeOption());
        parser.fill(model.getTradeTypeOption());
        parser.fill(model.getTradeNoOption());
        parser.fill(model.getLineNoOption());
        parser.fill(model.getDeliveryDateOption());
        parser.fill(model.getStoreCodeOption());
        parser.fill(model.getBuyerCodeOption());
        parser.fill(model.getPurchaseTypeCodeOption());
        parser.fill(model.getSellerCodeOption());
        parser.fill(model.getTenantCodeOption());
        parser.fill(model.getNetPriceTotalOption());
        parser.fill(model.getSellingPriceTotalOption());
        parser.fill(model.getShipmentStoreCodeOption());
        parser.fill(model.getShipmentSalesTypeCodeOption());
        parser.fill(model.getDeductionCodeOption());
        parser.fill(model.getAccountCodeOption());
        parser.fill(model.getOwnershipDateOption());
        parser.fill(model.getCutoffDateOption());
        parser.fill(model.getPayoutDateOption());
        parser.fill(model.getOwnershipFlagOption());
        parser.fill(model.getCutoffFlagOption());
        parser.fill(model.getPayoutFlagOption());
        parser.fill(model.getDisposeNoOption());
        parser.fill(model.getDisposeDateOption());
        return true;
    }
    @Override public void close() throws IOException {
        parser.close();
    }
}