select vendor_code, 
  vendor_name, 
  max(case when is_valid_order then created_date_local end) as last_valid_order, 
  max(case when is_gross_order then created_date_local end) as last_gross_order,
from `fulfillment-dwh-production.pandata_curated.pd_orders` 
where created_date_utc >= '2020-01-01' 
  and global_entity_id = 'FP_PK'
  and vendor_code in (
  'n8ps','u3nh','n3uz','t5zo','v1fa','n4dz','u1ix','n0ia','v3ij','v2rs','t4kz','t7mz','u4tn','t0nq','n8tu','n8qm','n7dr','n0ao','s6pl','t6pn','s4of','n2om','t4pc','n5jg','u2ws','n1yq','t7gb','v0me','v0gp','n1pf','n4gf','n1oc','v6zc','s1th','u2sf','n0yi','n1vo','t4zn','n2tr','u9eq','u4co','t0wr','u2kk','v8wj','n3bw','t1wf','u8nn','t1hm','v4fx','u2gt','v2lg','v9lg','u0gx','n8le','n4ig','v5xz','n8zd','u7vu','v8tc','w5na','u4bn','u6wu','u2er','n8go','u7vd','t6sh','u6qq','n0dl','u6lv','n8rv','n5ff','n9qc','u6aa','n8sh','u0us','u7zm','v6lr','u5ni','u6bn','n0ha','n6li','n2sr','n3nq','n5mh','n4zn','s9ov','w3ch','u7fa','u8ee','u8tg','w2kz','t0ts','n6ji','u3ew','n4ng','n5ai','v6jb','n2rw','t2ro','n1ai','u0ee','u6gb','u9is','n2cd','n3gu','lanu','siq8','z3dz','k4cj','wn0w','thxe','y6gd','v7rx','i8je','w1mb','n8ve','d8pc','v0pi','w8pe','w1al','u8mt','o8sj','t4xr','j0rz','ra14','v4fz','s4eg','s1xl','t3ry','s9gp','t4xi','u0ao','v1zm','e2em','s6sz','f7ux','k2uz','x4pr','v8rw','u5rw','u6co','s8cd','s7dl','p5xs','t6we','t0cp','u3np','y9cf','y7id','v4la','v8hf','q0az','r7sg','u5db','t9ur','t4ns','t1yq','w6js','t7ev','s0zs','u6mm','v2tb','s0ma','v0nu','s6bb','w3ge','z2hd','v6ya','t3pb','v1rs','v5qu','r7bw','t5hj','u0qe','w0zo','v7pn','t8ad','job9','u1on','ab21','u1nu','d5it','u2wn','g5oj','v1zs','v5zn','t6ak','t9cm','m6vm','s3ia','p1gm','s0jx','x1jl','t2ur','dpc1','t7tg','d9ul','s0ik','s9eq','s4gh','s9jz','t4wa','t3cc','v4dr','v1vi','s4bh','u1rj','s2ne','i9nm','m5hr','v7xx','w9uz','w7cy','s5vz','n9oi','t9mt','t6zb','z9kh','s0ob','t6hr','n1de','s6zc','u6ai','u1ye','s4ya','n6oi','t5fa','s9rk','q4bt','u9yw','s8ri','s3fs','s1il','t7tq','u3ow','t4us','w0xc','k4mx','t9hp','l5xr','v3in','t9oc','t5jc','s1wf','t2fp','t7vr','t4qw','t5br','v5hl','v5hl','m7ez','u8bf','t2wn','f3eq','v0vm','u3mk','euvd','t2yo','t6ab','k5jj','d9hk','m9mz','c0rj','b5fa','n3bk','u0se','u9zo','d0pu','t1hi','s2vh','u4dk','u4fu','u1cd','iz4j','s0af','s6rj','et5g','t7fb','u3dq','u6hu','t2vr','z6io','v9oe','s5cq','e7ow','t7jf','u3gp','u4ir','t4ih','b2oz','t0sr','s1ae','t5gt','w2tb','s8tm','u4ts','n9ee','u8jx','u6ew','n9ov','u3ey','v3br','u4yb','s8ze','u2fr','t0ux','n9ws','o4wl','v0zc','t2oc','mc4e','l73x','x4fc','s5gf','n3qk','u0uq','s8rb','v1ff','bhx3','u7ag','l1xs','u1jb','q5ja','t0mq','u8dt','s0is','u2gl','u7ap','u1rn','n7pv','n6cj','s4ls','s2ye','u0kk','e1zr','e1zr','u1mz','t9kk','t3td','v3tf','s0ue','u3bg','s8fk','t2zb','e7dt','n9bb','e9so','f8xg','t2ry','v4sn','s7et','u3du','j8ec','n4lw','s2ao','n7sx','u1yf','i2ql','f5gh','s3kd','swel','v8qk','u7rn','e4dv','s8kh','u2sc','g2cn','q8zu','v9pg','s0do','s4hy','u4nk','n5oj','h4eh','m4ha','s8ke','u6kn','g4md','u0kx','u3ca','s0og','u2za','s7mv','b6xq','u7mc','s9vx','y2ms','p7du','l5wf','i2nn','m8pe','a8dg','p9rv','d9do','q7ks','b4yy','n0ad','t1by','v2vw','s3ef','u4hh','u3bx','n7yx','u8fr','s0sb','u0ym','v1na','n3uy','ui4f','u8jh','u5te','u7gd','u7oj','n8ts','t9pg','n5ho','u4zk','u7jg','n6vc','n8sa','v7lx','t5kx','t2yy','u4de','s2wb','r8qe','t2ul','v9kd','t6wd','s3cw','t0xx','u3vt','n9uh','w4my','s3ah','v8au','v7gi','v8vp','s9tu','n7fb','v1el','w2tx','u1vd','xyox','u5co','v1gl','v2je','n4pw','u2oq','v2hj','u4on','s3lv','n3bx','u3sf','s9ek','s6zd','v8ra','u8nm','t5az','v9hm','u8hw','tza5','v7kn','u2re','n8dn','v4qz','v5av','w0yt','o5ip','u2cy','u6mt','o0ud','s9cp','s8dg','s4jp','u2oh','v5ww','v6ng','s1xv','s1on','v3rx','v2li','u3hp','t4ze','v2sm','v2ca','n0ki','n4ze','s6ko','v2dc','v0wv','v0cs','t9pp','s7ly','n3wc','v7hq','v0xo','v4fn','d3bt','v9yx','s0zm','x7yl','t9li','u9vp','u7et','t2kv','n1cz','u3gr','n4th','t1ti','v6ap','t7dj','u9at','t7qf','s1ih','k0uh','t7py','v5al','f7tr','s5xx','s0ho','s4py','s1pp','s1ox','s3du','s4af','s4im','s8zx','s9tm','s2qp','v8bl','t3ai','t4xb','t9sm','t2dn','t2yp','t7ai','u2yq','u3pu','g3lt','s7om','g3lt','qt6x','i59l','enj8','ssb1','os28','fnqf','aoma','hy95','qm91','forn','g0ep','ug2x','rxbx','lucs','cpeh','yfnz','v5e3','heuy','o8kg','ybdb','ao9n','i4w2','nylb','qjfi','q788','kj6c','lo3y','i7n4','s5lt','s6qp','p5xl','x8ne','xyzm','b2mb','rukt','mf1t','v4mg','ovpy','helo','e3yl','v3hz','v3mb','gjsu','x1m4','iawx','s6hz','s4gl','s8mb','s4gq','u3xf','u5xu','w0ox','n55o','t4as','t1kn','t3te','t5xl','t2zh','qcsn','wiud'
  )
group by 1,2