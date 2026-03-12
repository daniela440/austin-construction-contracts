# PDF Extraction (PyMuPDF + macOS Vision OCR)

Use when a government site publishes contract data as PDF files rather than HTML or JSON.

## When to use
- Source URL ends in `.pdf` or links to a PDF viewer
- Data is in a downloadable PDF report (bid tabs, contract awards, letting results)
- Standard text extraction works → use PyMuPDF only
- PDF uses Type3/custom fonts with no ToUnicode mapping → add Vision OCR

---

## Pattern A: Simple text extraction (PyMuPDF)

Works when the PDF has selectable/copyable text (most government PDFs).

```python
import fitz  # pip install pymupdf
import tempfile
import urllib.request

def download_pdf(url, ssl_ctx=None):
    """Download a PDF to a temp file. Returns path or None on 404."""
    from urllib.error import HTTPError
    req = urllib.request.Request(url, headers={"User-Agent": "Scraper/1.0"})
    try:
        kwargs = {"timeout": 120}
        if ssl_ctx:
            kwargs["context"] = ssl_ctx
        with urllib.request.urlopen(req, **kwargs) as resp:
            data = resp.read()
    except HTTPError as e:
        if e.code == 404:
            return None
        raise
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(data)
    tmp.close()
    return tmp.name

def extract_pdf_text(pdf_path):
    """Extract full text from all pages of a PDF."""
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text

# Usage
pdf_path = download_pdf("https://example.gov/contracts/awards.pdf")
if pdf_path:
    text = extract_pdf_text(pdf_path)
    # Parse text with regex
```

## Diagnosing extraction failure
If `page.get_text()` returns garbled or empty text, the PDF likely uses Type3 fonts.
Check with: `fitz.open(path)[0].get_fonts()` — look for `"Type3"` in the output.
→ If Type3 fonts are present, escalate to Pattern B (Vision OCR).

---

## Pattern B: Render page to PNG + macOS Vision OCR

Use when text extraction fails due to Type3/custom fonts (e.g., Hyland Cloud PDFs).
**macOS only** — requires Xcode command line tools (`swift` must be in PATH).

```python
import fitz
import os
import re
import subprocess
import tempfile

_SWIFT_OCR = '''\
import Vision
import AppKit

let imgURL = URL(fileURLWithPath: "IMG_PATH")
guard let img = NSImage(contentsOf: imgURL) else { exit(1) }
var imgRect = NSRect(origin: .zero, size: img.size)
guard let cgImg = img.cgImage(forProposedRect: &imgRect, context: nil, hints: nil) else { exit(1) }

var output = ""
let sema = DispatchSemaphore(value: 0)
let req = VNRecognizeTextRequest { req, _ in
    if let results = req.results as? [VNRecognizedTextObservation] {
        for obs in results { if let top = obs.topCandidates(1).first { output += top.string + "\\n" } }
    }
    sema.signal()
}
req.recognitionLevel = .accurate
try? VNImageRequestHandler(cgImage: cgImg).perform([req])
sema.wait()
print(output)
'''

def pdf_page_to_png(pdf_path, page_num=0, scale=2):
    """Render a PDF page to PNG at 2x resolution. Returns PNG path."""
    doc = fitz.open(pdf_path)
    pix = doc[page_num].get_pixmap(matrix=fitz.Matrix(scale, scale))
    png_path = pdf_path.replace(".pdf", f"_p{page_num}.png")
    pix.save(png_path)
    doc.close()
    return png_path

def ocr_png(png_path):
    """Run macOS Vision OCR on a PNG. Returns extracted text or '' on failure."""
    swift_code = _SWIFT_OCR.replace("IMG_PATH", png_path)
    try:
        result = subprocess.run(
            ["swift", "/dev/stdin"],
            input=swift_code,
            capture_output=True,
            text=True,
            timeout=45,
        )
        return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""

# Usage
png_path = pdf_page_to_png(pdf_path, page_num=0)
text = ocr_png(png_path)
os.unlink(png_path)

# Parse dollar amounts from OCR text
amounts = re.findall(r'\$([\d,]+\.\d{2})', text)
```

---

## SSL setup (macOS Python)

Many macOS Python installs lack default CA certs. Add this at the top of scrapers:

```python
import ssl
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE
```

---

## Install

```bash
pip install pymupdf certifi
# Vision OCR: no pip install — uses macOS system framework via swift subprocess
```

## Real examples in this project
- `tn_tdot_contracts.py` — Pattern A (text extraction from TDOT Contract Awards PDFs)
- `co_vss_contracts.py` — Pattern B (Type3 font PDFs from Hyland Cloud; Playwright downloads the PDF then OCR extracts bid amounts)
