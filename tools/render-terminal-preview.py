"""Render actual Ratatui TestBackend cells for visual review (requires Pillow)."""
import json, pathlib, sys, unicodedata
from PIL import Image, ImageDraw, ImageFont
source=pathlib.Path(sys.argv[1]); data=json.loads(source.read_text(encoding='utf-8'))
cw,ch=11,24
image=Image.new('RGB',(data['width']*cw,data['height']*ch));draw=ImageDraw.Draw(image)
latin=ImageFont.truetype('C:/Windows/Fonts/consola.ttf',18)
cjk=ImageFont.truetype('C:/Windows/Fonts/msyh.ttc',17)
previous=None
for cell in data['cells']:
 x,y=cell['x']*cw,cell['y']*ch
 if previous and cell['x']==previous['x']+1 and cell['y']==previous['y'] and any(unicodedata.east_asian_width(c) in ('W','F') for c in previous['text']):
  cell['bg']=previous['bg']
 draw.rectangle((x,y,x+cw-1,y+ch-1),fill=tuple(cell['bg']))
 previous=cell
skip=None
for cell in data['cells']:
 if skip==(cell['x'],cell['y']): continue
 text=cell['text']
 if any(unicodedata.east_asian_width(c) in ('W','F') for c in text): skip=(cell['x']+1,cell['y'])
 if text.strip():
  if len(text)==1 and 0x2581<=ord(text)<=0x2588:
   level=ord(text)-0x2580; x,y=cell['x']*cw,cell['y']*ch
   draw.rectangle((x,y+ch*(8-level)//8,x+cw-1,y+ch-1),fill=tuple(cell['fg']));continue
  font=cjk if any('\u2e80'<=c<='\u9fff' or '\uff00'<=c<='\uffef' for c in text) else latin
  draw.text((cell['x']*cw,cell['y']*ch+1),text,font=font,fill=tuple(cell['fg']))
output=source.with_suffix('.png'); image.save(output);print(output)
