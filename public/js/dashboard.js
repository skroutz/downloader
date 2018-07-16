const LivenessRoot = document.getElementById('js-liveness')
const AggregationsRoot = document.getElementById('js-aggregations')
const StatsRoot = document.getElementById('js-stats')

class Liveness extends React.Component {
    constructor(props) {
        super(props)

        this.staleAfter = props.staleAfter
        this.el = document.createElement('div')
        this.state = {
            status: 'alive',
        }
    }

    refresh() {
        this.setState((prevState, props) => {
            const now = new Date
            const status = (now - props.ping) > this.staleAfter ? 'dead' : 'alive'
            return {status: status}
        })
    }

    componentWillReceiveProps(nextProps) {
        if(this.props.ping != nextProps.ping) {
            this.refresh()
        }
    }

    componentDidMount() {
        LivenessRoot.appendChild(this.el)
        this.interval = setInterval(() => this.refresh(), this.staleAfter)
    }

    componentWillUnmount() {
        LivenessRoot.removeChild(this.el)
        clearInterval(this.interval)
    }

    render() {
        const cssClass = this.state.status == 'alive' ? 'success' : 'alert'
        return ReactDOM.createPortal(
            <div class={"callout " + cssClass}>
                {this.props.children}
            </div>,
            this.el,
        )
    }
}

class Aggregations extends React.Component {
    constructor(props) {
        super(props)
        this.every = props.every
        this.state = {}
    }

    fetch() {
        Hosts.forEach(async (h) => {
            const reply = await fetch(`${h}/dashboard/aggregations`)
            const json = await reply.json()
            let aggr = {}
            aggr[h] = json.map((a) => {
                a.host = h.replace("http://", "").replace("https://", "")
                return a
            })
            this.setState(aggr)
        })
    }

    componentDidMount() {
        this.fetch()
        this.interval = setInterval(() => this.fetch(), this.every)
    }

    componentWillUnmount() {
        clearInterval(this.interval)
    }

    render() {
        const hosts = Object.keys(this.state)

        let aggrs = []
        hosts.forEach((h, i) => {
            aggrs = aggrs.concat(this.state[h])
        })
        aggrs.sort((a, b) => b.size - a.size) // desc size

        if (aggrs.length == 0) {
            return (
                <div class="callout secondary">
                    <h5>No running aggregations :(</h5>
                </div>
            )
        }

        return (
            <table class="hover">
            <thead>
                <th>Aggregation</th>
                <th>Host</th>
                <th>Pending Downloads</th>
            </thead>
            <tbody>
                { aggrs.map((r) =>
                <tr>
                    <td>{r.name}</td>
                    <td>{r.host}</td>
                    <td>{r.size}</td>
                </tr>
                )}
            </tbody>
        </table>
        )
    }
}

class ProcessorStats extends React.Component {
    constructor(props) {
        super(props)
        this.every = props.every
        this.state = {}
    }

    fetch() {
        Hosts.forEach(async (h) => {
            const reply = await fetch(`${h}/stats/processor`)
            const stats = await reply.json()
            let stat = {}
            stat[h] = {
                stats: stats,
                lastPing: new Date(),
            }
            this.setState(stat)
        })
    }

    componentDidMount() {
        this.fetch()
        this.interval = setInterval(() => this.fetch(), this.every)
    }

    componentWillUnmount() {
        clearInterval(this.interval)
    }

    render() {
        const hosts = Object.keys(this.state)
        if (hosts.length == 0) {
            return (
                <div class="callout secondary">
                <h5>No Stats found :(</h5>
            </div>
            )
        }
        let keys = []
        for (var i = 0; i < hosts.length; i++) {
          keys.push(...Object.keys(this.state[hosts[i]].stats));
        }
        const stats = [...new Set(keys)];
        const rows = stats.map(
            (k) => [k, hosts.map((h) => this.state[h].stats[k])]
        )

        return (
            <table class="unstripped">
            <thead>
            <th>Stats</th>
                { hosts.map((h) => <th>{h}</th>)}
            </thead>
            <tbody>
                { rows.map((k) =>
                <tr>
                    <td>{k[0]}</td>
                    { k[1].map((h) => <td>{h}</td>)}
                </tr>
                )}
            </tbody>

            { hosts.map((h) =>
            <Liveness staleAfter={this.every+1000} ping={this.state[h].lastPing}>
                {h}
            </Liveness>)}
            </table>
        )
    }
}

ReactDOM.render(<Aggregations every={5000} />, AggregationsRoot)
ReactDOM.render(<ProcessorStats every={5000} />, StatsRoot)
