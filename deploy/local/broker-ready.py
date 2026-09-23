"""Wait for the explicitly declared local queue; never creates a remote queue."""
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

deadline = time.monotonic() + 60
while True:
    try:
        request = urllib.request.Request("http://elasticmq:9324", data=b"Action=GetQueueUrl&QueueName=ledgence&Version=2012-11-05",
                                         headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(request, timeout=2) as response:
            root = ET.fromstring(response.read(65536))
        urls = [element.text for element in root.iter() if element.tag.endswith("QueueUrl")]
        if urls == ["http://elasticmq:9324/000000000000/ledgence"]:
            break
    except (OSError, ET.ParseError):
        pass
    if time.monotonic() >= deadline:
        raise SystemExit("configured local ElasticMQ queue did not become ready")
    time.sleep(0.25)
