SELECT  p.vendor_code, 
    format_date('%B', p.created_date_local) as month,
    avg(case when o.is_preorder=false then o.actual_delivery_time_in_seconds/60 end) as dt,
    avg(o.vendor_late_in_seconds/60) as vendor_lateness,
    avg(o.assumed_actual_preparation_time_in_seconds/60) as assumed_actual_prep_time,
    avg(o.estimated_prep_time_in_seconds / 60) as estimated_prep_time,
    count(case when c.stacked_deliveries_count >= 1 then p.code end)/count(distinct p.code) as stacking,
FROM `fulfillment-dwh-production.pandata_curated.lg_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_orders` p
  on p.code = o.code and p.created_date_utc = o.created_date_utc and p.global_entity_id = o.global_entity_id
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on v.vendor_code = p.vendor_code
left join unnest(o.deliveries) as c on c.is_primary
 WHERE p.created_date_utc between "2021-07-01" - 1 and '2021-10-31' + 1
   and p.created_date_local between "2021-07-01" and '2021-10-31'
   and p.global_entity_id = 'FP_PK'
   and v.city = 'Lahore'
   and p.expedition_type = 'delivery'
   and p.is_own_delivery
   and not p.is_test_order
   and p.is_valid_order
   and v.vendor_code in ("v4ri","u7et","u1on","u8jx","f8xg","u0uq","u5ni","t1kn","swel","w0ox","n9il","b6xq","t5fa","n0cp","ra14","wfte","t4us","u3xf","iut8","m4u3","t47d","f3eq","wwjh","w0lz","t4qw","s9rk","t2fp","e9so","opf2","v0zc","p53l","v5hl","u1rj","s3yc","oxkq","n1de","n9ov","t2oc","s0zs","votg","m4m6","s2ot","u4qs","u4ed","n0ki","euvd","t2yo","t7vr","ybdb","u3gp","rxbx","c3ya","t5jc","y3cg","w7xf","v0fh","t5br","j8ec","u8bf","t9ll","s0ob","k0uh","p2wj","u4zk","uraa","u8ee","s4ya","u8jn","u7jg","s0jx","t2wn","u1bh","s71m","u0se","z2hd","p5xs","qvtk","t1yq","cpeh","v3rx","s1sb","u4hh","pbfd","q4bt","s0ma","t9cm","f3wf","w0zo","u57g","gtps","s4nu","g5oj","n5ho","u4on","u3ey","t0mq","qcsn","bo8a","s9eq","u4cw","u3lp","pfdu","v7kl","s7uh","v9pg","s9tu","v8qk","o5ip","fnqf","ug2x","xa0x","n3wf","heuy","n3wi","lo3y","s6hz","u7fa","n9qc","t3ec","t9oc","n4lw","ui4f","xww2","t4ja","et5g","ho2k","v3lf","s2ne","mav7","s9gm","er2n","fxfp","xcph","s3ym","s9ek","s2ww","rhfx","m0cg","tfbo","lucs","s5lp","forn","u8rm","s9mm","utt5","yekg","t7yc","s3ef","v3mb","n8sa","zc2j","b5fa","s0is","t2ry","mc4e","vs8f","u1cd","s8fk","bfxk","u4ts","t0ux","no8q","t4ni","f5gh","s8ze","t2ri","t0xx","t4xr","u2oh","s4nn","t5xh","u0gx","spwo","v3os","ml5b","nyzv","s5ca","v4zd","vrq0","iuye","hy95","h9yi","ehe4","i7n4","s2ao","w0at","z9kh","v2nt","t2zb","n8sh","v2dc","t6ak","l1xs","aoma","u7wu","t5kx","j7hf","n55o","u6hu","n1ai","g9jv","g0ep","sb3v","u8dt","v5e3","l46h","z3g8","n1vo","qkhx","u7mc","w0wm","n6qt","s7ur","qt6x","wiud","jjb8","n4yl","w0oy","gm47","cqqi","s0ik","x2ji","n8dd","siq8","qoht","t3ik","g7tv","a3dp","w1fc","j6ba","u9yw","p1cc","z3oa","t2yq","jorw","u6mt","v4er","s4bh","u2kk","u6li","k4jg","s3zz","n0nb","ssb1","vg0c","s8ay","u2fr","t8ad","c12f","s3ia","n8ab","n5oj","v0uq","u8fr","q8xh","v9lg","n1cz","t7qf","l4cv","n9uh","s2ju","r8qe","u2sc","lanu","t2lf","t3nh","x4fc","w2tx","egay","n8nv","awlk","u6co","d0zr","t7dj","u5rw","o8kg","p9nm","u0kx","w3lf","dr0p","sz13","ao9n","n8ve","s0do","u0ee","n0dl","fdri","ka7g","t7bh","u2oq","s8ke","u2re","r7bw","u7oj","u4co","mf1t","u7gd","t1wf","l73x","v2hx","u7yf","n3qc","gyo2","t7tq","xfjj","hhs6","u4de","x8ne","qpig","u4nk","v4fz","k5jj","p4nj","a0sk","d1gi","qq0y","n9ws","n07u","u3nu","t2zh","g3lt","t3td","w4iy","f3ov","v6gs","os28","v2ay","nb12","n6vc","u8nm","r8jf","ena4","u9vp","lvv1","p5xl","s9vx","unw5","i59l","qu0e","n3qk","n7co","n3bw","u8oz","u2oa","t1rb","u0uw","n2tr","n7fb","s1th","u7dc","n4ze","ms9m","n8bo","u3sf","u3hp","n1pf","t9kk","n2fg","w7cy","xfrh","s1hc","u2za","j0zt","t2ur","s3fs","qm91","v8wj","t1hi","xyzm","n3su","t9pe","t5xl","t4kz","v3eo","n3sc","t6hr","s6pl","i5ib","b2oz","job9","d1dr","v2sq","n8xj","n4me","g4ve","v6zc","u7xo","t5hj","bhx3","u3du","s5lt","m9zq","w5yd","u8nn","tza5","v0wq","wn0w","pokz","t4ns","t7jf","us4f","v1ff","u4tn","s0og","n3oy","u1rn","b2mb","a5mr","lem2","n2ie","t2vr","n3us","hdxb","v1fa","u3gr","t7gb","g6iy","u3dq","u3bg","bw20","t7tg","a3uj","d9ul","n2om","o4wl","n8tu","v9oy","rukt","u0kk","bjyp","u7zm","w0xc","n0ab","s6qp","xodx","n1yq","v3br","v0vm","n4th","ovpy","s0sb","e7dt","t5gt","n8dn","ptpd","n4dw","v2nx","u3mk","n9oi","yfnz","p3ax","s8tm","s2wb","w2aj","n5jg","mhb8","n0ao","t0wr","n8np","w1mb","v3in","s2vh","m7ez","w0qt","rde6","t0nq","u2ws","t9mt","g5yz","u1jb","n5sn","u3bx","j3ms","d5it","s6zc","v0pi","v3ij","enj8","s6zd","t4pc","s3lv","n0ia","t9pg","s3cw","o4yy","t9pp","s7ly","n7yx","d8pc","j0ug","k4cj","t4ih","s9se","t6sz","n2es","t5ao","n4dz","s1on","s8ri","t5zo","t6pn","n0ad","t5az","u5co","v9oe","bwo9","u6ai","mo0w","s5vz","iz4j","v7rx","n6cj","s1xv","t2ul","w8go","t7mz","u8hw","u3nh","u6mp","s9cp","xwmd","iawx","n0ls","s2ye","omt4","q8zu","gjsu","xyox","u4ul","hvvm","u6mm","s0af","s4hy","b80p","u5te","s6ko","s7mv","s6bb","v8wc","u4fu","t1by","s4ls","s5gf","pl20","s1ae","e1zr","v8rw","t7ev","t3pb","u0qe","ab21","u1nu","u2wn","dpc1","v1vi","v7xx","t6zb","u3ow","t9hp","t6ab","d9hk","n3bk","u9zo","u4dk","z6io","e7ow","u4ir","n9ee","u4yb","s8rb","u7ag","u2gl","u7ap","n7pv","u1mz","v3tf","s0ue","n7sx","u1yf","s8kh","u6kn","v2vw","v1na","n3uy","u8jh","n8ts","v7lx","t2yy","v9kd","t6wd","u3vt","s3ah","v7gi","v8vp","v1el","u1vd","v1gl","v2je","v2hj","v8ra","v9hm","v7kn","v4qz","v5av","w0yt","u2cy","v5ww","v6ng","v2li","t4ze","v2sm","v2ca","v0wv","v0cs","n3wc","v7hq","v0xo","v4fn","d3bt","v9yx","t9li","t2kv","v6ap","v5al","f7tr","v8bl","s7om","i4w2","nylb","qjfi","q788","kj6c","v4mg","helo","e3yl","v3hz","x1m4","s4gl","s8mb","s4gq","u5xu","t4as","t3te","k2rq","zjlo","ynm3","s6pq","gv1j","th1h","r2kq","rnr9","gy7j","eqgj")
group by 1,2