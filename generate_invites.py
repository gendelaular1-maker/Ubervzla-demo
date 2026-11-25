"""Run privately; never commit output or share codes publicly."""
import secrets, hashlib, argparse
p=argparse.ArgumentParser();p.add_argument('count',type=int,default=5,nargs='?');a=p.parse_args();assert 1<=a.count<=100
codes=[secrets.token_urlsafe(24) for _ in range(a.count)]
for i,c in enumerate(codes,1): print(f'Invitación {i}: {c}')
print('UBERVZLA_INVITE_HASHES='+','.join(hashlib.sha256(c.encode()).hexdigest() for c in codes))
