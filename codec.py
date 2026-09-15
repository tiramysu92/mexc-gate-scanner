"""Native protobuf descriptors: field numbers from MEXC's public schema.

Unknown fields are safely ignored by protobuf; required depth fields are
validated before the reconstruction layer uses them.
"""
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.internal import api_implementation
from .books import Delta


def message_class():
    if api_implementation.Type() not in ("upb", "cpp"):
        raise RuntimeError("Native protobuf (upb/cpp) required")
    file = descriptor_pb2.FileDescriptorProto(name="mexc_v3.proto", package="mexc_v3", syntax="proto3")
    def message(name, fields):
        m = file.message_type.add(name=name)
        for name, number, kind, repeat, target in fields:
            f = m.field.add(name=name, number=number, type=kind, label=3 if repeat else 1)
            if target:
                f.type_name = ".mexc_v3."+target
    message("Level", [("price",1,9,False,None),("quantity",2,9,False,None)])
    message("Depth", [("asks",1,11,True,"Level"),("bids",2,11,True,"Level"),
                      ("first",4,9,False,None),("last",5,9,False,None)])
    message("Order", [("order_id",1,9,False,None),("client_id",2,9,False,None),
        ("price",3,9,False,None),("quantity",4,9,False,None),("order_type",7,5,False,None),
        ("side",8,5,False,None),("executed",13,9,False,None),("quote",14,9,False,None),
        ("status",15,5,False,None),("created",16,3,False,None),("symbol",17,9,False,None)])
    message("Deal",[("price",1,9,False,None),("quantity",2,9,False,None),("amount",3,9,False,None),
        ("side",4,5,False,None),("trade_id",7,9,False,None),("client_id",8,9,False,None),
        ("order_id",9,9,False,None),("fee",10,9,False,None),("fee_asset",11,9,False,None),("time",12,3,False,None)])
    message("Envelope", [("channel",1,9,False,None),("symbol",3,9,False,None),
        ("created",5,3,False,None),("sent",6,3,False,None),
        ("order",304,11,False,"Order"),("deal",306,11,False,"Deal"),("depth",313,11,False,"Depth")])
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("mexc_v3.Envelope"))


Envelope = message_class()


def decode_depth(raw, received_ms):
    e = Envelope.FromString(raw)
    if not e.HasField("depth"):
        return None
    if not e.symbol or e.sent <= 0:
        raise ValueError("Missing depth identity/time")
    return Delta(e.symbol, int(e.depth.first), int(e.depth.last), e.sent, received_ms,
                 tuple((x.price,x.quantity) for x in e.depth.bids),
                 tuple((x.price,x.quantity) for x in e.depth.asks))


def decode_order(raw):
    e = Envelope.FromString(raw)
    if not e.HasField("order") or not e.symbol:
        return None
    o = e.order
    return {"symbol":e.symbol,"orderId":o.order_id,
        "clientOrderId":o.client_id,"executedQty":o.executed,"cummulativeQuoteQty":o.quote,
        "status":{1:"NEW",2:"FILLED",3:"PARTIALLY_FILLED",4:"CANCELED",5:"PARTIALLY_CANCELED"}.get(o.status,"UNKNOWN"),
        "side":{1:"BUY",2:"SELL"}.get(o.side,"UNKNOWN"),"sendTime":e.sent}


def decode_deal(raw):
    e = Envelope.FromString(raw)
    if not e.HasField("deal") or not e.symbol:
        return None
    d = e.deal
    return {"symbol":e.symbol,"order_id":d.order_id,"client_id":d.client_id,
        "trade_id":d.trade_id,"quantity":d.quantity,"amount":d.amount,
        "side":{1:"BUY",2:"SELL"}.get(d.side,"UNKNOWN"),"fee":d.fee,"fee_asset":d.fee_asset}
