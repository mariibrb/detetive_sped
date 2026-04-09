"""
Detetive Fiscal — interface Streamlit (dois SPED: Excel por ficheiro + comparativo NF-e/CT-e).

No mesmo repositório (pasta deste ficheiro):
  - detetive_core.py
  - spedlib/   (pacote completo)

Instalação (ex.: Streamlit Cloud):
  pip install -r requirements-streamlit.txt

Execução local:
  streamlit run app.py
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import detetive_core  # noqa: E402


def _bytes_upload(f) -> tuple[bytes, str]:
    if f is None:
        return b"", ""
    f.seek(0)
    return f.read(), f.name


def _css_rosa():
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; }
        h1 { color: #db2777 !important; }
        .stDownloadButton button { font-weight: 600; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main():
    st.set_page_config(
        page_title="Detetive Fiscal",
        page_icon="🕵️",
        layout="wide",
    )
    _css_rosa()

    st.title("🕵️ Detetive Fiscal")
    st.caption(
        "Comparativo de dois SPED: Excel por ficheiro (C190+D190) e relatório único NF-e + CT-e."
    )

    st.markdown("### Ficheiros")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader(detetive_core.ROTULO_NASCEL)
        fn = st.file_uploader(
            "SPED Nascel (.txt ou .xlsx)",
            type=["txt", "xlsx"],
            key="sped_nascel",
        )
    with c2:
        st.subheader(detetive_core.ROTULO_CLIENTE)
        fc = st.file_uploader(
            "SPED Cliente (.txt ou .xlsx)",
            type=["txt", "xlsx"],
            key="sped_cliente",
        )

    st.info(
        "Dois primeiros botões: um Excel **só** por SPED (C190+D190 quando houver). "
        "Terceiro: comparativo **NF-e + CT-e** → `Detetive_sped.xlsx`."
    )

    b1, b2, b3 = st.columns(3)
    with b1:
        go_n = st.button(f"Excel — {detetive_core.ROTULO_NASCEL}", use_container_width=True)
    with b2:
        go_c = st.button(f"Excel — {detetive_core.ROTULO_CLIENTE}", use_container_width=True)
    with b3:
        go_cmp = st.button("Excel comparativo — NF-e + CT-e", use_container_width=True)

    if go_n:
        data, nome = _bytes_upload(fn)
        if not data:
            st.error(f"Anexe o ficheiro {detetive_core.ROTULO_NASCEL}.")
        else:
            bio, msg = detetive_core.gerar_excel_cfop_um_sped_completo(
                io.BytesIO(data),
                nome,
                rotulo_lado=detetive_core.ROTULO_NASCEL,
            )
            if bio:
                st.download_button(
                    label="⬇️ Descarregar Detetive_CFOP_Resultado_Nascel.xlsx",
                    data=bio.getvalue(),
                    file_name="Detetive_CFOP_Resultado_Nascel.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            else:
                st.error(msg)

    if go_c:
        data, nome = _bytes_upload(fc)
        if not data:
            st.error(f"Anexe o ficheiro {detetive_core.ROTULO_CLIENTE}.")
        else:
            bio, msg = detetive_core.gerar_excel_cfop_um_sped_completo(
                io.BytesIO(data),
                nome,
                rotulo_lado=detetive_core.ROTULO_CLIENTE,
            )
            if bio:
                st.download_button(
                    label="⬇️ Descarregar Detetive_CFOP_Resultado_Cliente.xlsx",
                    data=bio.getvalue(),
                    file_name="Detetive_CFOP_Resultado_Cliente.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            else:
                st.error(msg)

    if go_cmp:
        dc, nc = _bytes_upload(fc), _bytes_upload(fn)
        if not dc[0] or not nc[0]:
            st.error("Para o comparativo, envie **os dois** SPED (Cliente e Nascel).")
        else:
            bio, msg = detetive_core.gerar_excel_cfop_comparativo_nfe_e_cte_dois_speds(
                io.BytesIO(dc[0]),
                io.BytesIO(nc[0]),
                dc[1],
                nc[1],
            )
            if bio:
                st.download_button(
                    label="⬇️ Descarregar Detetive_sped.xlsx",
                    data=bio.getvalue(),
                    file_name="Detetive_sped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            else:
                st.error(msg)


if __name__ == "__main__":
    main()
