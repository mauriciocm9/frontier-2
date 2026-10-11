"""Stateful differential fuzz: original vs our ALB. Each scenario starts with /__reset on both."""
import json,sys,time,subprocess,urllib.request,urllib.error,pathlib
O="https://caja-negra-production-c64f.up.railway.app"; OT="cn-p-abh_bog_2dyh_43mg-a1HEnyBUOpBJZuDq8v_d6D"
M="http://frontier-api-1214499893.us-east-1.elb.amazonaws.com"
MT=subprocess.run(["/opt/data/.local/bin/aws","--region","us-east-1","secretsmanager","get-secret-value","--secret-id","frontier-api-token","--query","SecretString","--output","text"],capture_output=True,text=True).stdout.strip()
LOG=pathlib.Path(__file__).resolve().parents[1]/"experimentos.jsonl"
cnt=sum(1 for _ in LOG.open())
def ctrl(p):
    r=0
    for ch in p.upper():
        d=str(ord(ch)-64) if "A"<=ch<="Z" else ch if "0"<=ch<="9" else ""
        for x in d: r=(10*r+int(x))%97
    return 98-r*100%97
def http(base,tok,path,raw=None,method="POST",ct="application/json"):
    h={"Content-Type":ct} if ct else {}
    if tok: h["Authorization"]="Bearer "+tok
    r=urllib.request.Request(base+path,data=raw,method=method,headers=h)
    try: x=urllib.request.urlopen(r,timeout=20)
    except urllib.error.HTTPError as e: x=e
    except Exception as e: return 0,"ERR "+str(e),None
    b=x.read().decode("utf-8","replace")
    return x.status,b,x.headers.get("X-Req")
def canon(b):
    try: return json.loads(b)
    except Exception: return b
def send(step,seq,hyp):
    global cnt
    k=step[0]
    if k=="reset": path,raw="/__reset",None
    elif k=="m": path,raw="/msg",json.dumps({"m":f"{step[1]};{ctrl(step[1])}"},ensure_ascii=False).encode()
    elif k=="raw": path,raw="/msg",json.dumps({"m":step[1]},ensure_ascii=False).encode()
    else: path,raw="/msg",step[1]
    a=http(O,OT,path,raw); b=http(M,MT,path,raw)
    cnt+=1
    try: req=raw.decode() if raw else None
    except Exception: req=repr(raw)
    with LOG.open("a") as f: f.write(json.dumps({"id":cnt,"seq":seq,"x_req":a[2],"hipotesis":hyp,"request":req,"status":a[0],"response":canon(a[1])},ensure_ascii=False)+"\n")
    time.sleep(0.05)
    return a,b
S=[]
def sc(name,hyp,*steps): S.append((name,hyp,steps))
M_=lambda s:("m",s)
for amt in ["1","99","100","101","9999999999","10000000000","9999999999999999999","9223372036854775807","9223372036854775808","18446744073709551616","0","00","007","-5","1.5","1e3","+5","5 ","٣","100000","100001","99999","0100000"]:
    sc(f"dep-{amt}",f"deposito monto {amt!r}",M_("ALTA;A"),M_(f"DEPOSITO;AC-0001;{amt}"),M_("CONSULTA;AC-0001"))
sc("accum-big","acumular saldo muy grande",M_("ALTA;A"),M_("DEPOSITO;AC-0001;9999999999"),M_("DEPOSITO;AC-0001;9999999999"),M_("CONSULTA;AC-0001"),M_("DEPOSITO;AC-0001;9999999999"),M_("CONSULTA;AC-0001"),M_("ALTA;B"),M_("TRANSFER;AC-0001;AC-0002;100000"),M_("CONSULTA;AC-0002"))
sc("accum-int64","overflow int64 por acumulacion",M_("ALTA;A"),*[M_("DEPOSITO;AC-0001;9999999999")]*5,M_("CONSULTA;AC-0001"))
sc("fmt-cents","formato saldo con centavos",M_("ALTA;A"),M_("DEPOSITO;AC-0001;1"),M_("CONSULTA;AC-0001"),M_("DEPOSITO;AC-0001;9"),M_("CONSULTA;AC-0001"),M_("DEPOSITO;AC-0001;90"),M_("CONSULTA;AC-0001"),M_("DEPOSITO;AC-0001;123456789"),M_("CONSULTA;AC-0001"))
sc("tr-limit","limite transfer exacto +-1",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;500000"),M_("TRANSFER;AC-0001;AC-0002;100000"),M_("TRANSFER;AC-0001;AC-0002;100001"),M_("TRANSFER;AC-0001;AC-0002;99999"),M_("CONSULTA;AC-0001"),M_("CONSULTA;AC-0002"))
sc("tr-insuf","saldo exacto +1 vs limite",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;50"),M_("TRANSFER;AC-0001;AC-0002;51"),M_("TRANSFER;AC-0001;AC-0002;50"),M_("TRANSFER;AC-0001;AC-0002;1"),M_("CONSULTA;AC-0001"))
sc("tr-order1","orden validacion: limite vs saldo vs cuenta inexistente",M_("ALTA;A"),M_("TRANSFER;AC-0001;AC-0002;200000"),M_("TRANSFER;AC-0001;AC-0002;5"),M_("TRANSFER;AC-0009;AC-0001;200000"),M_("TRANSFER;AC-0001;AC-0009;200000"),M_("TRANSFER;AC-0009;AC-0008;5"),M_("TRANSFER;AC-0001;AC-0009;5"),M_("TRANSFER;AC-0009;AC-0001;5"))
sc("tr-same","origen=destino distintos casos",M_("ALTA;A"),M_("TRANSFER;AC-0001;AC-0001;5"),M_("TRANSFER;AC-0001;ac-0001;5"),M_("TRANSFER;AC-0009;AC-0009;5"),M_("TRANSFER;AC-0001;AC-0001;0"),M_("TRANSFER;AC-0001;AC-0001;200000"))
sc("tr-amount-vs-same","orden E303/E304",M_("ALTA;A"),M_("TRANSFER;AC-0001;AC-0001;abc"),M_("TRANSFER;AC-0001;AC-0001;-1"))
sc("tr-zero-cuenta","monto 0 con cuenta inexistente",M_("ALTA;A"),M_("TRANSFER;AC-0001;AC-0009;0"),M_("DEPOSITO;AC-0009;0"),M_("DEPOSITO;AC-0009;5"),M_("DEPOSITO;AC-0000;5"))
for cid in ["ac-0001","AC-1","AC-00001","AC-0001 ","Ac-0001","AC0001","0001","AC-0000","AC--1","AC-abcd"," AC-0001"]:
    sc(f"id-{cid}",f"formato id {cid!r}",M_("ALTA;A"),M_(f"CONSULTA;{cid}"),M_(f"DEPOSITO;{cid};10"),M_("CONSULTA;AC-0001"))
for oid in ["OP-0001","op-0001","OP-1","OP-0000","OP-9999","OP-10000","xx"]:
    sc(f"anula-{oid}",f"anula formato {oid}",M_("ALTA;A"),M_("DEPOSITO;AC-0001;10"),M_(f"ANULA;{oid}"),M_(f"ANULA;{oid}"),M_("CONSULTA;AC-0001"))
sc("anula-overdraw","anula deposito tras gastar",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;1000"),M_("TRANSFER;AC-0001;AC-0002;900"),M_("ANULA;OP-0001"),M_("CONSULTA;AC-0001"),M_("CONSULTA;AC-0002"),M_("ANULA;OP-0002"),M_("CONSULTA;AC-0001"),M_("CONSULTA;AC-0002"),M_("ANULA;OP-0002"),M_("DEPOSITO;AC-0001;5"))
sc("anula-ids","ids de operacion tras anula/fallos",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;10"),M_("ANULA;OP-0001"),M_("DEPOSITO;AC-0001;10"),M_("TRANSFER;AC-0001;AC-0002;1000"),M_("TRANSFER;AC-0001;AC-0002;1"),M_("DEPOSITO;AC-0009;5"),M_("DEPOSITO;AC-0001;5"))
sc("anula-limit","anular transfer",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;100000"),M_("DEPOSITO;AC-0001;100000"),M_("TRANSFER;AC-0001;AC-0002;100000"),M_("TRANSFER;AC-0001;AC-0002;100000"),M_("ANULA;OP-0003"),M_("ANULA;OP-0003"),M_("CONSULTA;AC-0001"),M_("CONSULTA;AC-0002"))
for nm in ["a","Lucia","lucia","Lucía","ñandú","a b","a;b","x"*200,"x"*5000,"AC-0001","日本","\u0000","  ","a\tb","Lucia;Gomez"]:
    sc(f"alta-{nm[:12]!r}",f"alta nombre {nm[:12]!r}",M_(f"ALTA;{nm}"),M_("ALTA;Lucia"),M_("CONSULTA;AC-0001"))
sc("alta-many","muchas altas",*[M_(f"ALTA;U{i}") for i in range(12)],M_("CONSULTA;AC-0012"),M_("CONSULTA;AC-0013"))
sc("alta-dup","alta duplicada",M_("ALTA;Lucia"),M_("ALTA;Lucia"),M_("ALTA;lucia"),M_("CONSULTA;AC-0002"))
for c in ["0","1","00","01","99","100","-1","5X","","  ","2","98","97","+5","٣"]:
    sc(f"ctl-{c!r}",f"control {c!r}",("raw",f"ALTA;A;{c}"),("raw",f"CONSULTA;AC-0001;{c}"),("raw",f"PING;{c}"),("raw",f"DEPOSITO;AC-0001;5;{c}"))
for m in ["","PING","PING;","PING;;","PING;a;b","PING;1;2;3",";","; ;",";;",";PING;1","ALTA;","ALTA;;5","ALTA;A;","  PING;5","PING;5 ","ping;1","pInG;1","PING ;1","A","ALTA;A;B;C;5","CONSULTA","ALTA;A;99;","\n","PING;\n5","DEPOSITO;AC-0001;5;5;5"]:
    sc(f"msg-{m!r}",f"mensaje crudo {m!r}",("raw",m))
for k in ["PONG","ALTAS","ALT","CONSUL","DEPOSITAR","TRANSFERIR","TRANSFERENCIA","ANULAR","DEP","X","XY","XYZ","CONSULTAS","SALDO","SALDOS","BAJA","altaa","TRANS","ANUL","PIN","PINGG","DEPOSITOS","DEPOSIT","ANULAA","CONSULTO","TRANSFE","ANULO","PANG"]:
    sc(f"unk-{k}",f"sugerencia {k}",M_(f"{k};A"),M_(f"{k};A;B"),M_(f"{k}"))
for b in [b"",b"{}",b"[]",b"null",b"\"x\"",b"{",b"{\"m\":\"PING;1\"",b"{\"m\":\"PING;1\"}garbage",b"{\"m\":\"PING;1\",\"x\":1}",b"{\"m\":[\"PING;1\"]}",b"{\"m\":null}",b"{\"m\":1}",b"{\"m\":true}",b"{\"M\":\"PING;1\"}",b"{\"m\":\"PING;1\",\"m\":\"x\"}",b"\xff\xfe",b"{\"m\":\"\\ud800\"}",b"{\"m\":\"PING;\\u0031\"}",b"{\"m\":"+b"1"*1000+b"}",b"{\"m\":\""+b"A"*70000+b"\"}",b"{\"m\":\"PING;1\"}\n\n"]:
    sc(f"body-{b[:20]!r}",f"cuerpo {b[:30]!r}",("body",b))
sc("reset-ids","reset reinicia ids",M_("ALTA;A"),M_("DEPOSITO;AC-0001;5"),("reset",),M_("CONSULTA;AC-0001"),M_("ALTA;A"),M_("DEPOSITO;AC-0001;5"),M_("ANULA;OP-0001"))
sc("anula-self-chain","anular anulacion",M_("ALTA;A"),M_("DEPOSITO;AC-0001;5"),M_("ANULA;OP-0001"),M_("ANULA;OP-0002"),M_("DEPOSITO;AC-0001;5"))
sc("consulta-neg","saldo negativo tras anular",M_("ALTA;A"),M_("ALTA;B"),M_("DEPOSITO;AC-0001;100"),M_("TRANSFER;AC-0001;AC-0002;100"),M_("ANULA;OP-0001"),M_("CONSULTA;AC-0001"),M_("TRANSFER;AC-0001;AC-0002;1"),M_("DEPOSITO;AC-0001;50"),M_("CONSULTA;AC-0001"))
sc("saldos","comando SALDOS",M_("ALTA;A"),M_("SALDOS;AC-0001"),M_("SALDOS"),M_("SALDOS;AC-0001;AC-0002"))
only=sys.argv[1] if len(sys.argv)>1 else None
diffs=[]; n=0
for name,hyp,steps in S:
    if only and only not in name: continue
    send(("reset",),"fz-"+name,"reset")
    for i,st in enumerate(steps):
        a,b=send(st,"fz-"+name,hyp); n+=1
        ca,cb=(a[0],canon(a[1])),(b[0],canon(b[1]))
        if ca!=cb:
            diffs.append((name,i,str(st)[:200],a[:2],b[:2]))
            print("DIFF",name,i,str(st)[:150],"\n   orig",str(a[:2])[:300],"\n   mine",str(b[:2])[:300],flush=True)
        elif a[1]!=b[1]: diffs.append((name,i,str(st)[:200],"FMT",a[1][:200],b[1][:200]))
print("scenarios",len(S),"steps",n,"semantic/status diffs",sum(1 for d in diffs if d[3]!="FMT"),"fmt-only",sum(1 for d in diffs if d[3]=="FMT"))
json.dump(diffs,open("/opt/data/Frontier-2/pruebas/fuzz_diffs.json","w"),ensure_ascii=False,indent=1,default=str)
