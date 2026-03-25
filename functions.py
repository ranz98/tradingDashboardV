import json, logging, time, math

logger = logging.getLogger("TRADE_MGR")

def get_precision(symbol, binance_req):
    """Fetches price precision for a symbol from Binance."""
    try:
        info = binance_req("GET", "/fapi/v1/exchangeInfo", {})
        for s in info.get('symbols', []):
            if s['symbol'] == symbol:
                tick_size = float(next(f['tickSize'] for f in s['filters'] if f['filterType'] == 'PRICE_FILTER'))
                return max(0, -int(round(math.log10(tick_size))))
    except: pass
    return 8 # Default fallback

def nuke_all_orders(symbol, binance_req, add_event_log):
    """Cancels both standard and algo orders for a symbol."""
    # 1. Standard orders
    binance_req("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
    
    # 2. Algo orders (often used for internal SLs on some accounts)
    for status in ["ACTIVE", "NEW"]:
        try:
            algos = binance_req("GET", "/fapi/v1/algoOrders", {"symbol": symbol, "status": status})
            if isinstance(algos, dict): algos = algos.get("orders", [])
            for ao in algos:
                aid = ao.get("algoId")
                if aid: binance_req("DELETE", "/fapi/v1/algoOrder", {"algoId": aid})
        except: pass
    
    add_event_log(f"Nuked all orders for {symbol}")

def manage_open_trades(positions, managed_trades, binance_req, notify, inc_trades_exec, add_event_log):
    """
    Robust management: Closes 50% @ 25% profit, rounds entry, nukes, and sets SL (Standard -> Algo).
    """
    for p in positions:
        symbol = p["symbol"]
        side   = p["side"]
        entry  = p["entryPrice"]
        mark   = p["markPrice"]
        qty    = p["size"]
        pct    = p["changePct"]
        
        # 25% Profit Trigger
        if pct >= 25.0:
            # We only manage trades that haven't been 'managed' yet in this session
            if symbol in managed_trades:
                continue

            logger.info(f"🚀 {symbol} hit 25% profit! Managing trade...")
            add_event_log(f"Managing {symbol}: hit 25% profit.")
            
            # Mark IMMEDIATELY to prevent double execution if the next loop runs before this one finishes
            managed_trades[symbol] = {"be_set": False, "entry": entry}
            
            # 1. Close 50% of the position
            close_side = "SELL" if side == "LONG" else "BUY"
            half_qty = round(qty / 2, 3) 
            
            if half_qty > 0:
                resp = binance_req("POST", "/fapi/v1/order", {
                    "symbol": symbol, "side": close_side,
                    "type": "MARKET", "quantity": str(half_qty),
                })
                if "orderId" in resp:
                    notify(f"🎯 <b>{symbol} 25% PROFIT HIT</b>\nClosed half position (qty: {half_qty}). Moving SL to Break Even.")
                    inc_trades_exec()
                    add_event_log(f"Partial close executed: {symbol} (qty {half_qty})")
                else:
                    notify(f"⚠️ <b>{symbol} Partial Close Failed</b>\n{resp.get('msg', 'Unknown error')}")
            
            # 2. Cancel existing SL/TP orders for this symbol
            binance_req("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
            
            # 3. Place new Stop Loss at Entry (Break Even)
            sl_resp = binance_req("POST", "/fapi/v1/order", {
                "symbol": symbol, "side": close_side,
                "type": "STOP_MARKET", "stopPrice": str(entry),
                "closePosition": "true",
            })
            
            if "orderId" in sl_resp:
                managed_trades[symbol]["be_set"] = True
                logger.info(f"✅ {symbol} moved to BE successfully.")
                add_event_log(f"Moved SL to Break-Even: {symbol}")
            else:
                logger.warning(f"❌ {symbol} Move-to-BE failed: {sl_resp.get('msg')}")
                
            return True
                
    return False

def check_trade_closures(current_syms, active_targets, binance_req, notify, add_event_log):
    """
    Checks if any trades that were in our 'active_targets' are now gone.
    If so, identifies if it was a TP or SL hit and notifies.
    """
    # Symbols in active_targets but NOT in current positions are closed
    closed_syms = [s for s in active_targets if s not in current_syms]
    
    for symbol in closed_syms:
        # Search recent income/trades to see the final PNL
        # Or simpler: if it's gone, fetch last income for symbol
        income = binance_req("GET", "/fapi/v1/income", {"symbol": symbol, "limit": 1})
        if isinstance(income, list) and len(income) > 0:
            last = income[0]
            pnl = float(last.get("income", 0))
            if pnl > 0:
                notify(f"💰 <b>{symbol} TAKE PROFIT HIT</b>\nResult: <code>+{pnl:.2f} USDT</code>")
                add_event_log(f"Target Hit: {symbol} (+${pnl:.2f})")
            else:
                notify(f"🛑 <b>{symbol} STOP LOSS HIT</b>\nResult: <code>{pnl:.2f} USDT</code>")
                add_event_log(f"Target Hit: {symbol} (-${abs(pnl):.2f})")
        else:
            notify(f"ℹ️ <b>{symbol} Trade Closed</b>")
        
        # Remove from active_targets so we don't notify again
        del active_targets[symbol]
        return True # Signal state change

    return False
