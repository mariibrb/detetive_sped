"""
Detetive Fiscal — interface Streamlit (espelho da app Django).

Requisitos no mesmo repositório (mesma pasta que este ficheiro):
  - detetive_core.py
  - confronto_gerencial.py
  - spedlib/   (pacote completo)

Instalação (ex.: venv + Streamlit Cloud):
  pip install streamlit pandas openpyxl xlsxwriter tqdm

Execução local:
  streamlit run app.py
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import streamlit as st

# Garantir imports quando se corre a partir da raiz do projeto ou do GitHub
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import confronto_gerencial  # noqa: E402
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
        "Comparativo de SPED (NF-e + CT-e) e confronto Gerencial × SPED — mesmo motor que o projeto Django."
    )

    modo = st.radio(
        "Modo",
        options=["Dois SPED", "Gerencial × SPED"],
        horizontal=True,
    )

    if modo == "Dois SPED":
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

    else:
        st.markdown("### Gerencial × SPED")
        fs = st.file_uploader("SPED (.txt ou .xlsx)", type=["txt", "xlsx"], key="sped_g")
        fg = st.file_uploader("Planilha gerencial (.xlsx)", type=["xlsx"], key="plan_g")

        mov = st.selectbox("Movimento", ["Entradas", "Saídas"])
        usar_chaves = st.checkbox("Usar aba de chaves (Documento × Chave NF-e) no workbook", value=True)
        sh_ger = st.text_input("Aba gerencial (opcional)", placeholder="Vazio = automático")
        sh_chv = st.text_input("Aba chaves (opcional)", placeholder="Vazio = automático")

        if st.button("Gerar diagnóstico (Excel)", type="primary", use_container_width=True):
            data_sped, nome_sped = _bytes_upload(fs)
            data_ger, _nome_ger = _bytes_upload(fg)
            if not data_sped or not data_ger:
                st.error("Envie o SPED e a planilha gerencial (.xlsx).")
            else:
                bio, msg = confronto_gerencial.gerar_excel_confronto_gerencial(
                    io.BytesIO(data_sped),
                    nome_sped,
                    io.BytesIO(data_ger),
                    mov,
                    sheet_gerencial=sh_ger.strip() or None,
                    sheet_chaves=sh_chv.strip() or None,
                    usar_mapa_chaves=usar_chaves,
                )
                if bio:
                    st.download_button(
                        label="⬇️ Descarregar Detetive_Gerencial_x_SPED.xlsx",
                        data=bio.getvalue(),
                        file_name="Detetive_Gerencial_x_SPED.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
                else:
                    st.error(msg)


if __name__ == "__main__":
    main()
