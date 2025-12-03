# -*- coding = utf-8 -*-
# @Time: 2025-11-29 17:32:16
# @Author: Donvink
# @Site:
# @File: custom_poly_client.py
# @Software: PyCharm
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    TickSize,
    OrderArgs,
    PartialCreateOrderOptions,
    CreateOrderOptions,
)
from py_clob_client.utilities import price_valid

executor = ThreadPoolExecutor(max_workers=15)


class CustomClient(ClobClient):
    def get_order_signature_fields(
        self,
        token_id: str,
        tick_size: Optional[TickSize] = None,
        fee_rate_bps: Optional[float] = None,
        neg_risk: Optional[bool] = None,
    ):
        tasks = []
        if tick_size is None:
            tasks.append(executor.submit(self.get_tick_size, token_id))
        if fee_rate_bps is None:
            tasks.append(executor.submit(self.get_fee_rate_bps, token_id))
        if neg_risk is None:
            tasks.append(executor.submit(self.get_neg_risk, token_id))
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception:
                raise

    def create_order(self, order_args: OrderArgs, options: Optional[PartialCreateOrderOptions] = None, tick_size: Optional[TickSize] = None, fee_rate_bps: Optional[float] = None,
                     neg_risk: Optional[bool] = None,
                     ):
        self.get_order_signature_fields(order_args.token_id, tick_size, fee_rate_bps, neg_risk)
        self.assert_level_1_auth()

        # add resolve_order_options, or similar
        tick_size = self._ClobClient__resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        ) if tick_size is None else tick_size

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                "price ("
                + str(order_args.price)
                + "), min: "
                + str(tick_size)
                + " - max: "
                + str(1 - float(tick_size))
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else self.get_neg_risk(order_args.token_id)
        ) if neg_risk is None else neg_risk

        # fee rate
        fee_rate_bps = self._ClobClient__resolve_fee_rate(order_args.token_id, options.fee_rate_bps if options else None) if fee_rate_bps is None else fee_rate_bps
        order_args.fee_rate_bps = fee_rate_bps

        return self.builder.create_order(
            order_args,
            CreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )
