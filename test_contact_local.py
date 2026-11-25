import os,sys,tempfile,hashlib
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
with tempfile.TemporaryDirectory() as d:
 os.environ['UBERVZLA_FOUNDATION_DB']=d+'/test.db'; os.environ['UBERVZLA_VAULT_KEY']=Fernet.generate_key().decode();os.environ['UBERVZLA_INVITE_HASHES']=hashlib.sha256(b'fake-invite').hexdigest()
 sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)));import app as a
 h={'origin':a.PASSKEY_ORIGIN,'X-Portal':'driver'}
 with TestClient(a.app,base_url=a.PASSKEY_ORIGIN) as c:
  assert c.get('/health').status_code==200
  assert c.get('/driver').status_code==200
  reg={'invite':'fake-invite','name':'Test','login':'testuser','password':'fake-password-123','role':'driver','vehicle':'car'}
  assert c.post('/api/register',headers=h,json=reg).status_code==503
  # Mock only in test process: no real data or provider calls.
  a.vault=lambda:Fernet(os.environ['UBERVZLA_VAULT_KEY'].encode())
  assert c.post('/api/register',headers=h,json=reg).status_code==200
  profile={'full_name':'Test Person','identity':'V-00000000','phone':'+580000000000','email':'test@example.invalid','state':'Test','municipality':'Test','vehicle':'car','plate':'TEST123','license_expiry':'2099-01-01','medical_expiry':'2099-01-01','insurance_expiry':'2099-01-01','consent':True}
  assert c.post('/api/intake/profile',headers=h,json=profile).status_code==200
  assert c.post('/api/intake/contact/send',headers=h,json={'channel':'phone'}).status_code==503
  a.verify_config=lambda:('fake','fake','fake')
  def mock(endpoint,fields):
   if endpoint=='Verifications':return {'sid':'VE'+'a'*32,'status':'pending'}
   return {'sid':'VE'+'a'*32,'status':'approved' if fields['Code']=='111111' else 'pending','valid':fields['Code']=='111111'}
  a.verify_request=mock
  for channel in ('phone','email'):
   assert c.post('/api/intake/contact/send',headers=h,json={'channel':channel}).status_code==200
   assert c.post('/api/intake/contact/send',headers=h,json={'channel':channel}).status_code==429
   assert c.post('/api/intake/contact/check',headers=h,json={'channel':channel,'code':'222222'}).status_code==400
   assert c.post('/api/intake/contact/check',headers=h,json={'channel':channel,'code':'111111'}).status_code==200
  assert c.get('/api/intake/status',headers=h).json()['contacts']=={'phone':True,'email':True}
  assert c.post('/api/intake/file',headers=h,json={'kind':'identity','content':'/9j/'}).status_code==503
  os.environ['UBERVZLA_DOCUMENT_UPLOAD_ENABLED']='1'
  assert c.post('/api/intake/file',headers=h,json={'kind':'selfie','content':'/9j/'}).status_code==409
  profile['email']='other@example.invalid';assert c.post('/api/intake/profile',headers=h,json=profile).status_code==200
  assert c.get('/api/intake/status',headers=h).json()['contacts']=={'phone':False,'email':False}
  assert c.post('/api/intake/file',headers=h,json={'kind':'identity','content':'/9j/'}).status_code==409
  assert c.post('/api/availability',headers=h,json={}).status_code==403
  assert c.post('/api/withdraw-demo/start',headers=h,json={}).status_code==403
  print('PASS: gates, contact send and check with mock, resend limit, changed profile resets contacts, selfie order, work and withdrawal blocked')
