"""Registry de extractors do Conta Azul.

Cada entry mapeia o nome canonico da entidade -> classe Extractor.
DAGs declaram `ENTITIES = ["pessoas", ...]` com chaves deste dict.
Loader pega `EXTRACTORS[name]()` e delega.

Novo extractor:
1. Cria modulo extractors/<entidade>.py com subclass de Extractor.
2. Adiciona linha no dict abaixo.
3. Schema pydantic em schemas.py.
4. (Opcional) Adiciona list_<entidade>() ou get_<entidade> no client.py
   se o endpoint precisar de pattern especifico (default: client.paginate_all).
5. Documenta na tabela do README.md do conector.

Convencao do nome: pt-BR + plural, espelhando o endpoint da API
(`/pessoas` -> "pessoas", `/notas-fiscais` -> "notas_fiscais",
`/centro-de-custo` -> "centros_de_custo" — pluralizamos no raw).
"""
from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.extractors.baixas import BaixasExtractor
from connectors.conta_azul.extractors.categorias_dre import (
    CategoriasDreExtractor,
)
from connectors.conta_azul.extractors.categorias_financeiras import (
    CategoriasFinanceirasExtractor,
)
from connectors.conta_azul.extractors.centros_de_custo import (
    CentrosDeCustoExtractor,
)
from connectors.conta_azul.extractors.cobrancas import CobrancasExtractor
from connectors.conta_azul.extractors.conta_conectada import (
    ContaConectadaExtractor,
)
from connectors.conta_azul.extractors.contas_a_pagar import (
    ContasPagarExtractor,
)
from connectors.conta_azul.extractors.contas_a_receber import (
    ContasReceberExtractor,
)
from connectors.conta_azul.extractors.contas_financeiras import (
    ContasFinanceirasExtractor,
)
from connectors.conta_azul.extractors.contratos import ContratosExtractor
from connectors.conta_azul.extractors.contratos_detalhe import (
    ContratosDetalheExtractor,
)
from connectors.conta_azul.extractors.eventos_alteracoes import (
    EventosAlteracoesExtractor,
)
from connectors.conta_azul.extractors.notas_fiscais import (
    NotasFiscaisExtractor,
)
from connectors.conta_azul.extractors.notas_fiscais_itens import (
    NotasFiscaisItensExtractor,
)
from connectors.conta_azul.extractors.notas_servico import (
    NotasServicoExtractor,
)
from connectors.conta_azul.extractors.parcelas_detalhe import (
    ParcelasDetalheExtractor,
)
from connectors.conta_azul.extractors.pessoas import PessoasExtractor
from connectors.conta_azul.extractors.pessoas_detalhe import (
    PessoasDetalheExtractor,
)
from connectors.conta_azul.extractors.produto_categorias import (
    ProdutoCategoriasExtractor,
)
from connectors.conta_azul.extractors.produto_cest import (
    ProdutoCestExtractor,
)
from connectors.conta_azul.extractors.produto_ecommerce_marcas import (
    ProdutoEcommerceMarcasExtractor,
)
from connectors.conta_azul.extractors.produto_ncm import (
    ProdutoNcmExtractor,
)
from connectors.conta_azul.extractors.produto_unidades_medida import (
    ProdutoUnidadesMedidaExtractor,
)
from connectors.conta_azul.extractors.produtos import ProdutosExtractor
from connectors.conta_azul.extractors.produtos_detalhe import (
    ProdutosDetalheExtractor,
)
from connectors.conta_azul.extractors.saldo_atual import (
    SaldoAtualExtractor,
)
from connectors.conta_azul.extractors.saldo_inicial import (
    SaldoInicialExtractor,
)
from connectors.conta_azul.extractors.servicos import ServicosExtractor
from connectors.conta_azul.extractors.servicos_detalhe import (
    ServicosDetalheExtractor,
)
from connectors.conta_azul.extractors.transferencias import (
    TransferenciasExtractor,
)
from connectors.conta_azul.extractors.vendas import VendasExtractor
from connectors.conta_azul.extractors.vendas_detalhe import (
    VendasDetalheExtractor,
)
from connectors.conta_azul.extractors.vendas_itens import (
    VendasItensExtractor,
)
from connectors.conta_azul.extractors.vendedores import VendedoresExtractor


EXTRACTORS: dict[str, type[Extractor]] = {
    # Pessoas
    PessoasExtractor.NAME: PessoasExtractor,
    PessoasDetalheExtractor.NAME: PessoasDetalheExtractor,
    ContaConectadaExtractor.NAME: ContaConectadaExtractor,
    # Catalogo
    ProdutosExtractor.NAME: ProdutosExtractor,
    ProdutosDetalheExtractor.NAME: ProdutosDetalheExtractor,
    ProdutoCategoriasExtractor.NAME: ProdutoCategoriasExtractor,
    ProdutoNcmExtractor.NAME: ProdutoNcmExtractor,
    ProdutoCestExtractor.NAME: ProdutoCestExtractor,
    ProdutoUnidadesMedidaExtractor.NAME: ProdutoUnidadesMedidaExtractor,
    ProdutoEcommerceMarcasExtractor.NAME: ProdutoEcommerceMarcasExtractor,
    ServicosExtractor.NAME: ServicosExtractor,
    ServicosDetalheExtractor.NAME: ServicosDetalheExtractor,
    # Financeiro — master data
    CategoriasFinanceirasExtractor.NAME: CategoriasFinanceirasExtractor,
    CategoriasDreExtractor.NAME: CategoriasDreExtractor,
    CentrosDeCustoExtractor.NAME: CentrosDeCustoExtractor,
    ContasFinanceirasExtractor.NAME: ContasFinanceirasExtractor,
    # Financeiro — eventos
    ContasReceberExtractor.NAME: ContasReceberExtractor,
    ContasPagarExtractor.NAME: ContasPagarExtractor,
    TransferenciasExtractor.NAME: TransferenciasExtractor,
    SaldoInicialExtractor.NAME: SaldoInicialExtractor,
    SaldoAtualExtractor.NAME: SaldoAtualExtractor,
    EventosAlteracoesExtractor.NAME: EventosAlteracoesExtractor,
    # Financeiro — granularidade fina (N+1 a partir de eventos)
    ParcelasDetalheExtractor.NAME: ParcelasDetalheExtractor,
    BaixasExtractor.NAME: BaixasExtractor,
    CobrancasExtractor.NAME: CobrancasExtractor,
    # Comercial
    VendedoresExtractor.NAME: VendedoresExtractor,
    VendasExtractor.NAME: VendasExtractor,
    VendasDetalheExtractor.NAME: VendasDetalheExtractor,
    VendasItensExtractor.NAME: VendasItensExtractor,
    ContratosExtractor.NAME: ContratosExtractor,
    ContratosDetalheExtractor.NAME: ContratosDetalheExtractor,
    # Fiscal
    NotasFiscaisExtractor.NAME: NotasFiscaisExtractor,
    NotasFiscaisItensExtractor.NAME: NotasFiscaisItensExtractor,
    NotasServicoExtractor.NAME: NotasServicoExtractor,
}


__all__ = [
    "Extractor",
    "EXTRACTORS",
    "PessoasExtractor",
    "PessoasDetalheExtractor",
    "ContaConectadaExtractor",
    "ProdutosExtractor",
    "ProdutosDetalheExtractor",
    "ProdutoCategoriasExtractor",
    "ProdutoNcmExtractor",
    "ProdutoCestExtractor",
    "ProdutoUnidadesMedidaExtractor",
    "ProdutoEcommerceMarcasExtractor",
    "ServicosExtractor",
    "ServicosDetalheExtractor",
    "CategoriasFinanceirasExtractor",
    "CategoriasDreExtractor",
    "CentrosDeCustoExtractor",
    "ContasFinanceirasExtractor",
    "ContasReceberExtractor",
    "ContasPagarExtractor",
    "TransferenciasExtractor",
    "SaldoInicialExtractor",
    "SaldoAtualExtractor",
    "EventosAlteracoesExtractor",
    "ParcelasDetalheExtractor",
    "BaixasExtractor",
    "CobrancasExtractor",
    "VendedoresExtractor",
    "VendasExtractor",
    "VendasDetalheExtractor",
    "VendasItensExtractor",
    "ContratosExtractor",
    "ContratosDetalheExtractor",
    "NotasFiscaisExtractor",
    "NotasFiscaisItensExtractor",
    "NotasServicoExtractor",
]
