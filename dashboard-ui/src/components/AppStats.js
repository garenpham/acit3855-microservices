/* eslint-disable react-hooks/exhaustive-deps */
import React, { useEffect, useState } from 'react';
import '../App.css';

export default function AppStats() {
	const [isLoaded, setIsLoaded] = useState(false);
	const [stats, setStats] = useState({});
	const [error, setError] = useState(null);

	const getStats = () => {
		fetch(`http://messager.eastus2.cloudapp.azure.com:8100/stats`)
			.then((res) => res.json())
			.then(
				(result) => {
					console.log('Received Stats');
					setStats(result);
					setIsLoaded(true);
				},
				(error) => {
					setError(error);
					setIsLoaded(true);
				}
			);
	};
	useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return () => clearInterval(interval);
	}, [getStats]);

	if (error) {
		return <div className={'error'}>Error found when fetching from API</div>;
	} else if (isLoaded === false) {
		return <div>Loading...</div>;
	} else if (isLoaded === true) {
		return (
			<div>
				<h1>Latest Stats</h1>
				<table className={'StatsTable'}>
					<tbody>
						<tr>
							<th>Check In</th>
							<th>Booking Confirm</th>
						</tr>
						<tr>
							<td># CI: {stats['num_ci_readings']}</td>
							<td># BC: {stats['num_bc_readings']}</td>
						</tr>
						<tr>
							<td colspan="2">
								Max number of people stay: {stats['max_numPeople']}
							</td>
						</tr>
						<tr>
							<td colspan="2">
								Max number of nights spend: {stats['max_numNights']}
							</td>
						</tr>
					</tbody>
				</table>
				<h3>Last Updated: {stats['last_updated']}</h3>
			</div>
		);
	}
}
