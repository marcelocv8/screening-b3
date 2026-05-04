# Screening B3 — Minervini + Patterns

Screener automatizado de momentum para ações brasileiras, ETFs e BDRs, baseado nas metodologias de **Mark Minervini (SEPA/Trend Template)**, **Qullamaggie (Wedges de Momentum)** e padrões clássicos de análise técnica.

## Funcionalidades

- **Execução automática diária** às 16:20 BRT (via GitHub Actions)
- **Universo completo:** Todas as ações B3 + ETFs + BDRs (análise no underlying estrangeiro)
- **6 padrões técnicos detectados:** VCP, Wedge Momentum, Cup & Handle, Double Bottom, Inverse Head & Shoulders, Breakout
- **Scoring ponderado** com classificação Tier S/A/B/C
- **Interface web** hospedada gratuitamente no Streamlit Cloud
- **Filtros interativos** por tier, categoria e padrões

## Arquitetura

```
GitHub Actions (Cloud)
    ↓ 16:20 BRT, dias úteis
run_screening.py
    ↓
Yahoo Finance (OHLCV histórico)
Brapi (listagem de tickers)
Fundamentus (fundamentalistas BR)
    ↓
results/latest.parquet
    ↓
Streamlit Cloud (app.py)
    ↓
Seu navegador (qualquer dispositivo)
```

## Configuração

### 1. Fork/Clone este repositório

```bash
git clone https://github.com/marcelocv8/screening-b3.git
cd screening-b3
```

### 2. Instalar dependências (local)

```bash
pip install -r requirements.txt
```

### 3. Executar screening manualmente

```bash
python run_screening.py
```

### 4. Executar interface local

```bash
streamlit run app.py
```

### 5. Hospedar no Streamlit Cloud (recomendado)

1. Conecte seu GitHub ao [Streamlit Cloud](https://streamlit.io/cloud)
2. Deploy o repositório `screening-b3`
3. O app ficará disponível em uma URL pública

### 6. Configurar GitHub Actions

O workflow já está em `.github/workflows/daily_screening.yml`. Para funcionar corretamente:

1. Vá em **Settings > Secrets and variables > Actions**
2. Adicione o secret `BRAPI_TOKEN` com seu token da [brapi.dev](https://brapi.dev) (opcional, mas recomendado para acesso a todas as ações)

## Detecção de Padrões

| Padrão | Descrição | Peso |
|--------|-----------|------|
| VCP | Volatility Contraction Pattern (Minervini) | 3.0 |
| Wedge Momentum | Consolidação wedge com momentum preservado | 2.5 |
| Cup & Handle | Copo e cabo semanal (O'Neil) | 2.5 |
| Double Bottom | Fundo duplo após correção | 2.0 |
| Inverse H&S | Ombro-Cabeça-Ombro invertido | 2.0 |
| Breakout | Rompimento de resistência com volume ≥150% | Destaque 🔥 |

## Sistema de Scoring

### Trend Template (Base)
- Preço acima das MMEs 50, 150, 200
- Alinhamento das médias móveis
- Força relativa vs IBOV
- Próximo da máxima de 52 semanas

### Fundamentalistas (SEPA)
- Crescimento de receita/lucro
- ROE > 15%
- Endividamento saudável

### Classificação
| Tier | Score | Ação |
|------|-------|------|
| S | ≥ 13.0 | Candidatos elite |
| A | 9.5 – 12.9 | Fortes, observação ativa |
| B | 6.5 – 9.4 | Em evolução |
| C | < 6.5 | Fora do modelo |

## Limitações

- **Dados históricos:** Yahoo Finance (gratuito, ~15-20 min de delay)
- **BDRs:** Mapeamento via CSV público; BDRs recém-listados podem não estar mapeados
- **VCP/Wedge:** Detecção heurística aproximada; validação visual no ProfitChart recomendada
- **Fundamentus:** Scraping pode quebrar se o site mudar o layout

## Licença

MIT — Uso pessoal e educacional.

**Aviso:** Este software é apenas para fins informativos e educacionais. Não constitui recomendação de investimento. Sempre faça sua própria análise antes de operar.
