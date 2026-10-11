"""Randomized stateful differential fuzz (original vs ALB). usage: fuzz_random.py SEED N_SCENARIOS"""
import json,sys,time,random,pathlib
__file__=str(pathlib.Path(__file__).resolve())
src=open(pathlib.Path(__file__).parent/"fuzz_diff.py").read().split("S=[]")[0]
exec(src)
seed=int(sys.argv[1]) if len(sys.argv)>1 else 1
N=int(sys.argv[2]) if len(sys.argv)>2 else 100
rnd=random.Random(seed)
AMTS=["1","2","5","10","99","100","101","250","999","1000","50000","99999","100000","100001","100002","150000","999999","1000000","9999999999","10000000000","0","00","01","-1","1.0","1,5","1e2","abc","","١","1 ","0x10","+1","９９"]
ACCTS=["AC-0001","AC-0002","AC-0003","AC-0004","AC-0009","AC-0000","ac-0001","AC-1","AC-00001","AC-0001 ","AC-","AC","","AC-0002;AC-0001"]
OPS=["OP-0001","OP-0002","OP-0003","OP-0004","OP-0005","OP-0006","OP-0009","OP-0000","op-0001","OP-1","OP-00001","OP-","OP-0001 "]
NAMES=["Ana","Bob","lucia","Lucía","a","x"*50,"ß","ı","İ","ǆ","ﬁ","mañana","a b","  a","a  ","AC-0001","OP-0001","123","-1","é","日本語","😀","\t","a\u00a0b","ΑΒΓ","straße"]
KINDS=["ALTA","CONSULTA","DEPOSITO","TRANSFER","ANULA","PING","SALDOS","alta","Deposito","tRaNsFeR","ANULA","CONSULTA","DEPOSITO","TRANSFER"]
def mk():
    k=rnd.choice(KINDS)
    ku=k.upper()
    if ku=="ALTA": a=[rnd.choice(NAMES)]
    elif ku=="CONSULTA": a=[rnd.choice(ACCTS)]
    elif ku=="DEPOSITO": a=[rnd.choice(ACCTS),rnd.choice(AMTS)]
    elif ku=="TRANSFER": a=[rnd.choice(ACCTS),rnd.choice(ACCTS),rnd.choice(AMTS)]
    elif ku=="ANULA": a=[rnd.choice(OPS)]
    elif ku=="PING": a=[str(rnd.randint(0,99))]
    else: a=[rnd.choice(ACCTS)]
    # arity mutation
    r=rnd.random()
    if r<0.04 and a: a=a[:-1]
    elif r<0.08: a=a+[rnd.choice(["x","1",""])]
    prefix=";".join([k]+a)
    r=rnd.random()
    if r<0.85: return ("m",prefix)
    if r<0.93: return ("raw",f"{prefix};{(ctrl(prefix)+rnd.randint(1,5))%100}")
    return ("raw",prefix+";"+rnd.choice(["","7","100","-3","ab","007"]))
# bias: make most scenarios useful: create accounts + deposit first
def scenario():
    steps=[("m","ALTA;A"),("m","ALTA;B")]
    if rnd.random()<.7: steps.append(("m",f"DEPOSITO;AC-0001;{rnd.choice(['100','5000','100000','250000','9999999999'])}"))
    if rnd.random()<.4: steps.append(("m","ALTA;C"))
    for _ in range(rnd.randint(8,22)): steps.append(mk())
    return steps
diffs=[];n=0
for si in range(N):
    steps=scenario(); name=f"rnd{seed}-{si}"
    send(("reset",),"fz-"+name,"reset")
    for i,st in enumerate(steps):
        a,b=send(st,"fz-"+name,"fuzz aleatorio seed %d"%seed); n+=1
        if a[0]==429: time.sleep(1.2); a=http(O,OT,"/__reset") if False else a
        ca,cb=(a[0],canon(a[1])),(b[0],canon(b[1]))
        if a[0]==429:
            print("RATE-LIMITED orig; scenario unreliable",name,i,flush=True); break
        if ca!=cb:
            diffs.append({"scn":name,"step":i,"req":list(st),"orig":[a[0],a[1][:300]],"mine":[b[0],b[1][:300]],"history":[list(s) for s in steps[:i]]})
            print("DIFF",name,i,st,"\n   orig",str(a[:2])[:300],"\n   mine",str(b[:2])[:300],flush=True)
            break  # state diverged; stop this scenario
    time.sleep(0.3)
print("seed",seed,"scenarios",N,"steps",n,"diffs",len(diffs))
json.dump(diffs,open(f"/opt/data/Frontier-2/pruebas/fuzz_random_{seed}.json","w"),ensure_ascii=False,indent=1)
