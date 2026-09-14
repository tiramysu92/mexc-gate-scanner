"""Offline execution tests. All exchange calls are controlled fakes."""
import copy
import pathlib
import tempfile
import types
import unittest
from unittest.mock import patch
from test_public_flow import load_offline

HERE=pathlib.Path(__file__).resolve().parent


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.m=load_offline(HERE/'app.py')
        self.m.now_ms=lambda:1000000
        self.m.mexc_now_ms=lambda:1000000
        self.route={'id':'USDT>CTO>USD1','path':['USDT','CTO','USD1'],'symbols':['CTOUSDT','CTOUSD1']}
        self.market={'symbol':'CTOUSDT','base':'CTO','quote':'USDT','baseAssetPrecision':2,'apiRulesAvailable':True}
        self.meta={'CTOUSDT':self.market}
        self.cap={'symbol':'CTOUSDT','side':'SELL','order_type':'MARKET','amount_mode':'quantity'}
        self.m.state['depth']['CTOUSD1']={'ready':True,'ts':999990,'send_ts':999985,'bids':{.00105:50000},'asks':{.00106:50000}}
        self.m._public_symbol_usable=lambda s:True
        self.calls=[];self.results=[];self.updates=[];self.now=0
        self.free=100000;self.last=None
        self.m._live_prepare_followup_order=lambda *a,**k:0
        self.m._live_order_update=lambda *a,**k:self.updates.append((a,k))
        self.m._live_attempt_update=lambda *a,**k:None
        def submit(attempt,leg,purpose,spec,test,**kw):
            if spec.get('_post_guard'):spec['_post_guard'](1000000)
            self.calls.append(copy.copy(spec))
            item=self.results.pop(0)
            if isinstance(item,Exception):raise item
            status,executed,fee,known=item
            if executed=='all':executed=float(spec['quantity'])
            order={'symbol':spec['symbol'],'clientOrderId':spec['client_order_id'],
                'orderId':'order'+str(len(self.calls)),'side':'SELL','status':status,
                'executedQty':executed,'cummulativeQuoteQty':executed*.001}
            self.last=order
            return {'order':order,'commissions':{'CTO':fee} if fee else {},'commission_known':known}
        self.m._submit_live_order=submit
        self.m.live_order_status_client=types.SimpleNamespace(order=lambda *a:copy.deepcopy(self.last))
        self.m.live_account_client=types.SimpleNamespace(account=lambda:{'canTrade':True,'balances':[{'asset':'CTO','free':str(self.free)}]})

    def unwind(self,remaining=100,balances=None):
        return self.m._bounded_emergency_unwind('6ffdaa1aa4b545209d9058accda38e76',self.route,
            remaining,balances or {},self.meta,self.cap,.01,{})

    def test_cto_age_before_post_is_rejected_with_confirmation_headroom(self):
        # Observed ~105 ms at L2 minus ~74 ms since L1 submission => ~31 ms.
        self.m.state['depth']['CTOUSD1'].update(ts=999969,send_ts=999952)
        self.m.live_leg1_confirm_ms.append(69)
        with self.assertRaisesRegex(self.m.MexcSignalExpired,'exit_local_headroom'):
            self.m._entry_exit_headroom(self.route)
        self.assertEqual(self.m.LIVE_LEG2_BBO_AGE_MS,100)
        self.assertEqual(self.m.LIVE_LEG2_EXCHANGE_AGE_MS,150)

    def test_fresh_exit_passes_and_slower_measured_confirmation_blocks(self):
        self.assertEqual(self.m._entry_exit_headroom(self.route)['budget_ms'],85)
        self.m.live_leg1_confirm_ms.extend([120]*10)
        with self.assertRaises(self.m.MexcSignalExpired):self.m._entry_exit_headroom(self.route)

    def test_exchange_headroom_is_independently_required(self):
        self.m.state['depth']['CTOUSD1']['send_ts']=999920
        with self.assertRaisesRegex(self.m.MexcSignalExpired,'exit_exchange_headroom'):
            self.m._entry_exit_headroom(self.route)

    def test_headroom_rechecked_at_entry_guard(self):
        self.m.live_arm_status=lambda:{'effective':True}
        self.m._live_health_status=lambda r:(None,0,0,0,0)
        self.m.state['depth']['CTOUSD1']['ts']=999960
        with self.assertRaisesRegex(self.m.MexcSignalExpired,'exit_local_headroom'):
            self.m._entry_price_guard(self.route,{'order_type':'FILL_OR_KILL'},999990,20)

    def test_canceled_zero_then_filled_uses_new_client_id(self):
        self.results=[('CANCELED',0,0,True),('FILLED','all',0,True)]
        records,used,output,error,spec=self.unwind(19801.66)
        self.assertEqual(len(self.calls),2)
        self.assertNotEqual(self.calls[0]['client_order_id'],self.calls[1]['client_order_id'])
        self.assertAlmostEqual(used,19801.66)
        self.assertIsNone(error)

    def test_repeated_cancellation_stops_at_two_and_records_failure(self):
        self.results=[('CANCELED',0,0,True)]*3
        records,used,output,error,spec=self.unwind()
        self.assertEqual(len(self.calls),2)
        self.assertEqual(used,0)
        self.assertIn('CANCELED',str(error))

    def test_uncertain_first_exit_never_sells_again(self):
        self.results=[self.m.MexcOrderUncertain('timeout')]
        with self.assertRaises(self.m.MexcOrderUncertain):self.unwind()
        self.assertEqual(len(self.calls),1)

    def test_disagreeing_rest_never_sells_again(self):
        self.results=[('CANCELED',0,0,True)]
        self.m.live_order_status_client.order=lambda *a:dict(self.last,executedQty=1)
        with self.assertRaises(self.m.MexcOrderUncertain):self.unwind()
        self.assertEqual(len(self.calls),1)

    def test_missing_rest_confirmation_never_sells_again(self):
        self.results=[('CANCELED',0,0,True)]
        self.m.live_order_status_client.order=lambda *a:(_ for _ in ()).throw(TimeoutError())
        with self.assertRaises(self.m.MexcOrderUncertain):self.unwind()
        self.assertEqual(len(self.calls),1)

    def test_partial_fill_and_base_fee_are_subtracted_before_retry(self):
        self.results=[('PARTIALLY_CANCELED',60,.03,True),('FILLED','all',0,True)]
        records,used,output,error,spec=self.unwind()
        self.assertAlmostEqual(float(self.calls[1]['quantity']),39.97)
        self.assertAlmostEqual(used,100)
        self.assertIsNone(error)

    def test_partial_unknown_fee_blocks_second_exit(self):
        self.results=[('PARTIALLY_CANCELED',10,0,False)]
        records,used,output,error,spec=self.unwind()
        self.assertEqual(len(self.calls),1)
        self.assertIn('commission inconnue',str(error))

    def test_retry_preserves_preexisting_inventory(self):
        self.free=70
        self.results=[('CANCELED',0,0,True),('FILLED','all',0,True)]
        records,used,output,error,spec=self.unwind(balances={'CTO':{'total':20}})
        self.assertEqual(float(self.calls[1]['quantity']),50)
        self.assertAlmostEqual(used,50)
        self.assertIsNotNone(error)

    def test_retry_cannot_increase_to_free_balance(self):
        self.free=1e6
        self.results=[('PARTIALLY_CANCELED',10,0,True),('FILLED','all',0,True)]
        records,used,output,error,spec=self.unwind()
        self.assertEqual(float(self.calls[1]['quantity']),90)

    def test_expired_retry_window_prevents_second_post(self):
        self.results=[('CANCELED',0,0,True)]
        with patch.object(self.m.time,'monotonic',side_effect=[0,2]):
            records,used,output,error,spec=self.unwind()
        self.assertEqual(len(self.calls),1)
        self.assertIn('expirée',str(error))

    def test_full_real_attempt_records_open_exposure_after_two_cancellations(self):
        self.run_full_attempt(success_retry=False)

    def test_full_real_attempt_records_recovery_after_second_exit_fills(self):
        self.run_full_attempt(success_retry=True)

    def run_full_attempt(self,success_retry):
        # Exercise the route-level unwind integration, durable journal and
        # accounting. Deliberately bypass entry to test a post-purchase fault.
        m=self.m
        with tempfile.TemporaryDirectory() as tmp:
            m.LIVE_DB_PATH=str(pathlib.Path(tmp)/'live.db');m.TRADING_MODE='live'
            m.initialize_live_journal()
            try:
                # Rebind original functions to this module's journal/globals.
                original=load_offline(HERE/'app.py')
                for name in ['_live_prepare_followup_order','_live_order_update','_live_attempt_update']:
                    fn=getattr(original,name);setattr(m,name,types.FunctionType(fn.__code__,m.__dict__,name,fn.__defaults__))
                m._entry_price_guard=lambda *a:None
                m._live_health_status=lambda r:(None,0,0,0,0)
                m.live_order_spec=lambda *a,**k:{'symbol':'CTOUSDT','side':'BUY','order_type':'FILL_OR_KILL','quantity':'19801.66','quote_order_qty':None,'price':'0.001008','client_order_id':a[6]}
                m._route_capability=lambda r:{'leg1':{},'leg2':{},'unwind':self.cap}
                m._capability_matches_policy=lambda c:True
                m.depth_walk=lambda *a,**k:None
                m._asset_mark_usd=lambda *a:.001001
                m.stable_mark_usdt=lambda *a:1
                def reconcile(result,spec):
                    if spec['side']=='BUY':
                        result['commissions']={'USDT':.00998003664};result['commission_known']=True
                m._reconcile_order_commissions=reconcile
                m.state['depth']['CTOUSD1'].update(ts=999895,send_ts=999878)
                unwind_submit=m._submit_live_order
                def submit(a,leg,purpose,spec,test,**kw):
                    if purpose!='leg1':
                        result=unwind_submit(a,leg,purpose,spec,test,**kw)
                    else:
                        result={'order':{'symbol':'CTOUSDT','clientOrderId':spec['client_order_id'],'orderId':'entry','status':'FILLED','side':'BUY','executedQty':19801.66,'cummulativeQuoteQty':19.96007328},'commissions':{},'commission_known':False,'confirmed_ts_ms':1000000,'latency_ms':73,'confirm_ms':69,'request_started_ts_ms':999931}
                    executed,quote=m._order_numbers(result['order'])
                    m._live_order_update(spec['client_order_id'],result['order']['status'],final=True,executed_qty=executed,cumulative_quote_qty=quote)
                    return result
                m._submit_live_order=submit;self.results=[('CANCELED',0,0,True),('FILLED','all',0,True) if success_retry else ('CANCELED',0,0,True)]
                q={'route':self.route,'decision_id':'cto-fixture','quality':{'grade':'A'},'decision_ts_ms':999990,'gateway_admit_ts_ms':999990}
                status,*_=m._live_real_attempt(q,'cto-test',{'start_mark':1,'net':.05},q['quality'],19.960079,{},self.meta)
                row=m.live_db_execute('SELECT * FROM live_attempts_v240 WHERE attempt_id=?',('cto-test',),one=True)
                self.assertIn('105 ms',row['error'])
                if success_retry:
                    self.assertEqual(status,'forced_unwind')
                    self.assertAlmostEqual(row['unwind_input'],19762.05)
                    self.assertAlmostEqual(row['residual_mid'],39.61)
                    self.assertIsNone(row['unwind_error'])
                    self.assertEqual(m.live_db_execute('SELECT COUNT(*) n FROM live_open_exposures_v246',one=True)['n'],0)
                else:
                    self.assertEqual(status,'open_exposure')
                    self.assertAlmostEqual(row['residual_mid'],19801.66)
                    self.assertIn('CANCELED',row['unwind_error'])
                    self.assertTrue(m.live_runtime['circuit_open'])
                self.assertEqual(m.live_db_execute('SELECT COUNT(*) n FROM live_orders_v240',one=True)['n'],3)
            finally:m.live_db_connection.close()


if __name__=='__main__':unittest.main(verbosity=2)
