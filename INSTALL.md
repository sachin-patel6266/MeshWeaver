# Installation and first run

1. Install Python 3.10+ and open this folder in VS Code.
2. In PowerShell, create and activate a virtual environment:

   ```powershell
   py -3 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. Install MeshWeaver and its test dependencies:

   ```powershell
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   python -m pip install -e .
   ```

4. Create an identity per node with `python -m meshweaver init --identity-dir
   .\nodes\node1 --name node1`. Exchange each `identity.crt` out of band.
5. Create one trust bundle for the cluster using `meshweaver trust-bundle
   --output .\cluster-trust.pem --certs <all trusted identity.crt files>`.
   Copy it to every member; do not include identities you do not trust to run
   code.
6. Start a node with `python -m meshweaver node --identity-dir
   .\nodes\node1 --trusted-certs .\cluster-trust.pem --port 8765`. Start other
   nodes in additional terminals with `--bootstrap 127.0.0.1:8765`.
7. Submit work using the `submit` command documented in [README.md](README.md),
   or call `await node.submit(function, ...)` from an asyncio application.

For a complete isolated ten-node run, use `python .\scripts\run_demo.py`.

